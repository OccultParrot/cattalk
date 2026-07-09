import os
import time
import threading
import audioop
import logging
import discord
from discord.ext import voice_recv
from discord import opus
from discord.opus import OpusError
import dotenv

logging.disable()

dotenv.load_dotenv()

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


class Bot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.all())
        self.voice_client = None
        self.voice_recv = None
        self.tree = discord.app_commands.CommandTree(self)

    async def setup_hook(self):
        await self.tree.sync()

    async def on_ready(self):
        print(f"Logged in as {self.user}")


bot = Bot()
SPEAKING_TIMEOUT = 0.3
POLL_INTERVAL = 0.05
# RMS threshold for 16-bit PCM audio; raise this if background noise still
# triggers "started speaking", lower it if quiet speech gets missed.
VOLUME_THRESHOLD = 500


class SpeakingTracker(voice_recv.AudioSink):
    def __init__(self):
        super().__init__()
        self.last_packet = {}
        self.users = {}
        self.speaking = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        # write() runs off the asyncio loop, so timeouts are detected
        # from a separate polling thread instead of an async task.
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()

    def wants_opus(self):
        # False means we want PCM audio to decode and analyze for volume.
        return False

    def write(self, user, data):
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
                        print(f"{self.users.get(uid, uid)} stopped speaking")
            except Exception as e:
                print(f"poll loop error: {e!r}")

    def is_speaking(self, user_id):
        return user_id in self.speaking

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

    vc = await interaction.user.voice.channel.connect(cls=voice_recv.VoiceRecvClient)
    sink = SpeakingTracker()
    vc.listen(sink)
    bot.voice_client = vc
    bot.voice_recv = sink

    await interaction.response.send_message(f"Joined {interaction.user.voice.channel.name}", ephemeral=True)


@bot.event
async def on_voice_state_update(member, before, after):
    if bot.voice_client is None:
        return

    if after.channel is None and before.channel == bot.voice_client.channel:
        # User left the voice channel
        if not bot.voice_recv.is_speaking(member.id):
            print(f"{member} stopped speaking (left the channel)")
        else:
            print(f"{member} is still speaking (left the channel)")

    elif after.channel == bot.voice_client.channel and before.channel != bot.voice_client.channel:
        # User joined the voice channel
        print(f"{member} joined the voice channel")


if __name__ == "__main__":
    bot.run(os.getenv("DISCORD_TOKEN"))
