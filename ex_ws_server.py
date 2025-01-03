import asyncio
import random
from time import sleep

import websockets.exceptions

from logger import create_logger
from threading import Thread
from websockets.asyncio.server import serve
import redis.asyncio as redis

logger, listener = create_logger('logger', 'DEBUG')
listener.start()


async def echo(websocket):
    async for message in websocket:
        await websocket.send(message)


async def hello(websocket):
    name = await websocket.recv()
    print(f"<<< {name}")
    greeting = f"Hello {name}!"
    await websocket.send(greeting)
    print(f">>> {greeting}")


async def start_thread(websocket, user, data, other=False):
    if other:
        new_thread = Thread(target=asyncio.run, args=[stuff_doer2(websocket, user, data)], name=f'new_thread{random.randrange(100)}')
        new_thread.start()
    else:
        new_thread = Thread(target=asyncio.run, args=[stuff_doer(websocket, user, data)], name=f'new_thread{random.randrange(100)}')
        new_thread.start()
    await websocket.send('thread started!')
    print(f'outta stuff!')


async def fire_redis_event(websocket):
    try:
        while True:
            data = await websocket.recv()
            print(f'<<< data: {data}')
            if isinstance(data, str):
                user = f'user{random.randrange(1000000)}'
                if data.startswith('calc'):
                    await start_thread(websocket, user, data)
                    await websocket.send('published!')
                    continue
                elif data.startswith('other'):
                    await start_thread(websocket, user, data, True)
            await websocket.send(str(data))
    except websockets.exceptions.ConnectionClosedOK:
        print(f'connection closed..')


async def stuff_doer(websocket, user, data):
    redis_obj = redis.Redis(host='172.31.81.236', port=6379)
    pubsub = redis_obj.pubsub()
    await pubsub.psubscribe(user)
    await redis_obj.publish('ssm-channel', str({"user": user, "num": data.split(" ")[-1]}))
    while True:
        message = await pubsub.get_message(ignore_subscribe_messages=True)
        if message is not None:
            decoded_msg = message['data'].decode()
            await websocket.send(decoded_msg)
            if decoded_msg == 'STOP':
                break
    return str(random.randrange(100))


async def stuff_doer2(websocket, user, data):
    redis_obj = redis.Redis(host='172.31.81.236', port=6379)
    pubsub = redis_obj.pubsub()
    await pubsub.psubscribe(user)
    await redis_obj.publish('other-channel', str({"user": user, "message": data}))
    while True:
        message = await pubsub.get_message(ignore_subscribe_messages=True)
        if message is not None:
            decoded_msg = message['data'].decode()
            await websocket.send(decoded_msg)
            if decoded_msg == 'STOP':
                break
    return str(random.randrange(100))


async def main():
    async with serve(fire_redis_event, "localhost", 8765) as server:
        print(f'beginning server...')
        await server.serve_forever()
        print(f'server ended')

if __name__ == "__main__":
    asyncio.run(main())
