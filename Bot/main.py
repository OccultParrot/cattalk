import os
import time
import asyncio
import threading
import audioop
import json
import logging
import discord
import websockets
from discord.ext import voice_recv
from discord import opus
from discord.opus import OpusError
import dotenv

# Filter out discord.py's info logs
logging.basicConfig(level=logging.WARNING)

dotenv.load_dotenv()

WS_URL = os.getenv("WS_URL")

# Patching discord.py's opus decode to filter out corrupt packets
_original_decode = opus.Decoder.decode
_decode_error_count = 0
_decode_total_count = 0
def _safe_decode(self, data, *, fec=False):
    global _decode_error_count, _decode_total_count
    _decode_total_count += 1
    try:
        return _original_decode(self, data, fec=fec)
    except OpusError as e:
        _decode_error_count += 1
        if _decode_error_count % 50 == 0:
            print(f"Corrupted packets: {_decode_error_count}/{_decode_total_count} "
                  f"({_decode_error_count / _decode_total_count:.1%})")
        return b""
opus.Decoder.decode = _safe_decode


class WebSocketClient:
    def __init__(self, url):
        self.url = url
        self.ws = None
        self._lock = asyncio.Lock()

    async def connect_loop(self):
        while True:
            try:
                async with websockets.connect(self.url) as ws:
                    self.ws = ws
                    print(f"Connected to websocket server at {self.url}")
                    await ws.wait_closed()
            except Exception as e:
                print(f"WebSocket connection error: {e!r}")
            finally:
                self.ws = None
            await asyncio.sleep(2)  # backoff before reconnecting

    async def send(self, message):
        async with self._lock:
            if self.ws is None:
                print(f"WebSocket not connected, dropping message: {message}")
                return
            try:
                await self.ws.send(message)
            except Exception as e:
                print(f"WebSocket send error: {e!r}")


ws_client = WebSocketClient(WS_URL)


class Bot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.all())
        self.voice_client = None
        self.voice_recv = None
        self.tree = discord.app_commands.CommandTree(self)
        self.loop_ref = None

    async def setup_hook(self):
        # Scheduling tasks on the main loop
        self.loop_ref = asyncio.get_running_loop()
        self.loop_ref.create_task(ws_client.connect_loop())
        self.loop_ref.create_task(voice_watchdog())
        await self.tree.sync()

    async def on_ready(self):
        print(f"Logged in as {self.user}")


bot = Bot()
SPEAKING_TIMEOUT = 0.3
POLL_INTERVAL = 0.05
# Raise this to filter out more background noise
VOLUME_THRESHOLD = 500

# If we don't have more packets in this space of time, drop out and rejoin
AUDIO_STALL_TIMEOUT = 30
WATCHDOG_INTERVAL = 10


def _send_from_thread(message):
    """Schedule a websocket send from a non-asyncio thread (audio callback / poll loop)."""
    if bot.loop_ref is None:
        return
    asyncio.run_coroutine_threadsafe(ws_client.send(message), bot.loop_ref)


async def _join_channel(channel):
    """Connect to a voice channel, attach a fresh sink, and sync the server on who's there."""
    vc = await channel.connect(cls=voice_recv.VoiceRecvClient)
    sink = SpeakingTracker()
    vc.listen(sink)
    bot.voice_client = vc
    bot.voice_recv = sink

    payload = json.dumps({
        "type": "begin",
        "users": [
            {"name": member.display_name, "id": member.id}
            for member in channel.members
        ],
    })
    await ws_client.send(payload)
    return vc


async def voice_watchdog():
    """Detects a silently-dead voice receive session and rejoins to recover it. """
    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL)
        vc = bot.voice_client
        sink = bot.voice_recv
        if vc is None or sink is None or not vc.is_connected():
            continue

        channel = vc.channel
        human_members = [m for m in channel.members if not m.bot]
        stale_for = sink.seconds_since_last_packet()

        if human_members and stale_for > AUDIO_STALL_TIMEOUT:
            print(
                f"No audio packets received for {stale_for:.0f}s in "
                f"{channel.name} with {len(human_members)} member(s) present - "
                f"reconnecting voice session"
            )
            try:
                await vc.disconnect(force=True)
            except Exception as e:
                print(f"Error disconnecting stale voice client: {e!r}")

            try:
                await _join_channel(channel)
                print(f"Rejoined {channel.name} after stalled audio session")
            except Exception as e:
                print(f"Failed to rejoin {channel.name}: {e!r}")


class SpeakingTracker(voice_recv.AudioSink):
    def __init__(self):
        super().__init__()
        self.last_packet = {}
        self.users = {}
        self.speaking = set()
        # Catches every packet even if they are silent, so we know if we are still connected
        self.last_any_packet = time.time()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()

    def wants_opus(self):
        # False means we want PCM audio to decode and analyze for volume.
        return False

    def write(self, user, data):
        with self._lock:
            self.last_any_packet = time.time()
        try:
            volume = audioop.rms(data.pcm, 2)
            if volume < VOLUME_THRESHOLD:
                return

            now = time.time()
            with self._lock:
                was_speaking = user.id in self.speaking
                self.last_packet[user.id] = now
                self.users[user.id] = user
                if not was_speaking:
                    self.speaking.add(user.id)
                    _send_from_thread(json.dumps({"type": "start", "id": user.id}))
                    print(f"{user} started speaking")
        except Exception as e:
            print(f"write() error for {user}: {e!r}")

    def _poll_loop(self):
        while not self._stop.is_set():
            time.sleep(POLL_INTERVAL)
            try:
                now = time.time()
                with self._lock:
                    timed_out = [
                        uid for uid in self.speaking
                        if now - self.last_packet.get(uid, 0) >= SPEAKING_TIMEOUT
                    ]
                    for uid in timed_out:
                        self.speaking.discard(uid)
                        _send_from_thread(json.dumps({"type": "stop", "id": uid}))
                        print(f"{self.users.get(uid, uid)} stopped speaking")
            except Exception as e:
                print(f"poll loop error: {e!r}")

    def is_speaking(self, user_id):
        with self._lock:
            return user_id in self.speaking

    def seconds_since_last_packet(self):
        with self._lock:
            return time.time() - self.last_any_packet

    def forget(self, user_id):
        # Drop a user who's no longer in the channel so state doesn't grow forever
        with self._lock:
            self.last_packet.pop(user_id, None)
            self.users.pop(user_id, None)
            self.speaking.discard(user_id)

    def cleanup(self):
        self._stop.set()
        self.last_packet.clear()
        self.users.clear()
        self.speaking.clear()


@bot.tree.command(name="joinvc", description="Join your voice channel and track who's speaking")
async def joinvc(interaction: discord.Interaction):
    if interaction.user.voice is None:
        await interaction.response.send_message("You're not in a voice channel.", ephemeral=True)
        return

    await _join_channel(interaction.user.voice.channel)
    await interaction.response.send_message(f"Joined {interaction.user.voice.channel.name}", ephemeral=True)


@bot.event
async def on_voice_state_update(member, before, after):
    if bot.voice_client is None:
        return

    if after.channel is None and before.channel == bot.voice_client.channel:
        # User left the voice channel
        if bot.voice_recv.is_speaking(member.id):
            # They were still marked as speaking - tell the server they stopped
            # before telling it they left, so no one gets stuck "speaking" forever.
            await ws_client.send(json.dumps({"type": "stop", "id": member.id}))
            print(f"{member} stopped speaking (left the channel)")
        else:
            print(f"{member} is still speaking (left the channel)")

        bot.voice_recv.forget(member.id)
        await ws_client.send(json.dumps({"type": "leave", "id": member.id}))
        print(f"{member} left the voice channel")

    elif after.channel == bot.voice_client.channel and before.channel != bot.voice_client.channel:
        # User joined the voice channel
        payload = json.dumps({"type": "join", "id": member.id, "name": member.display_name})
        await ws_client.send(payload)
        print(f"{member} joined the voice channel")


if __name__ == "__main__":
    bot.run(os.getenv("DISCORD_TOKEN"))
