import asyncio
import json

import websockets
from attr import asdict, dataclass
from websockets.asyncio.server import serve

"""

We will get:
User started speaking
User stopped speaking
User joined
User left

Users in call when bot joins

"""


@dataclass
class User:
    name: str
    id: int
    is_speaking: bool = False


# All users in call
users = {}
# All connected websockets
clients = set()


async def broadcast(payload):
    if not clients:
        return
    message = json.dumps(payload)
    # Gather so one closed connection doesn't stop the others from receiving the message
    results = await asyncio.gather(
        *(conn.send(message) for conn in clients),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, Exception):
            print(f"Failed to send to a client: {result}")


async def handler(websocket: websockets.ServerConnection):
    clients.add(websocket)
    # Inform new clients so that they don't have to wait for a new event before seeing anything
    await websocket.send(json.dumps({
        "type": "sync",
        "users": [asdict(u) for u in users.values()],
    }))

    try:
        async for message in websocket:
            # Every message is a single JSON object: {"type": "...", ...fields}
            try:
                data = json.loads(message)
                msg_type = data["type"]
            except (json.JSONDecodeError, KeyError, TypeError):
                print(f"Invalid message: {message}")
                continue

            if msg_type == "start":
                # { "type": "start", "id": 0 }
                try:
                    user_id = int(data["id"])
                    users[user_id].is_speaking = True
                except (KeyError, ValueError, TypeError):
                    print(f"Invalid start message: {data}")
                    continue

                await broadcast({"type": "start", "id": user_id})
            elif msg_type == "stop":
                # { "type": "stop", "id": 0 }
                try:
                    user_id = int(data["id"])
                    users[user_id].is_speaking = False
                except (KeyError, ValueError, TypeError):
                    print(f"Invalid stop message: {data}")
                    continue

                await broadcast({"type": "stop", "id": user_id})
            elif msg_type == "join":
                # { "type": "join", "id": 0, "name": "string" }
                try:
                    user_id = int(data["id"])
                    users[user_id] = User(data["name"], user_id)
                except (KeyError, ValueError, TypeError):
                    print(f"Invalid join message: {data}")
                    continue

                await broadcast({"type": "join", "id": user_id, "name": users[user_id].name})
            elif msg_type == "leave":
                # { "type": "leave", "id": 0 }
                try:
                    user_id = int(data["id"])
                    users.pop(user_id)
                except (KeyError, ValueError, TypeError):
                    print(f"Invalid leave message: {data}")
                    continue

                await broadcast({"type": "leave", "id": user_id})
            elif msg_type == "begin":
                # { "type": "begin", "users": [{ "name": "string", "id": 0 }, ...] }
                try:
                    user_list = data["users"]
                except (KeyError, TypeError):
                    print(f"Invalid begin message: {data}")
                    continue

                # Updates the user list so that there are no stale users that left while bot was disconnected
                new_users = {}
                for u in user_list:
                    try:
                        # Skip the bot
                        if int(u["id"]) == 1524215863157329961:
                            continue
                        new_users[int(u["id"])] = User(u["name"], int(u["id"]))
                    except (ValueError, TypeError, KeyError):
                        continue
                users.clear()
                users.update(new_users)
                print(users)

                await broadcast({
                    "type": "sync",
                    "users": [asdict(u) for u in users.values()],
                })
            else:
                print(f"Invalid message type: {msg_type}")
                continue
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        # Drop dead clients
        clients.discard(websocket)


async def main():
    async with serve(handler, "localhost", 8765) as server:
        print("WebSocket server started on ws://localhost:8765")
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
