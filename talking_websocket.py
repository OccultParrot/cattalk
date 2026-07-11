import asyncio

import websockets
from websockets.asyncio.server import serve

"""

We will get:
User started speaking
User stopped speaking
User joined
User left

Users in call when bot joins

"""


async def handler(websocket: websockets.ServerConnection):
    async for message in websocket:
        if message.startswith("speaking:"):
            user_id = message.split(":", 1)[1]
            print(f"User {user_id} started speaking")
        elif message.startswith("stopped:"):
            user_id = message.split(":", 1)[1]
            print(f"User {user_id} stopped speaking")
        elif message.startswith("joined:"):
            user_id = message.split(":", 1)[1]
            print(f"User {user_id} joined the voice channel")
        elif message.startswith("left:"):
            user_id = message.split(":", 1)[1]
            print(f"User {user_id} left the voice channel")
        else:
            print(f"Unknown message: {message}")


async def main():
    async with serve(handler, "localhost", 8765) as server:
        print("WebSocket server started on ws://localhost:8765")
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
