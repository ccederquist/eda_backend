import asyncio
import json
from websockets.asyncio.client import connect
import redis.asyncio as redis
from concurrent.futures import ProcessPoolExecutor

async def hello():
    async with connect("ws://localhost:8765") as websocket:
        await websocket.send('calc 10')
        message = await websocket.recv()
        print(message)


async def calculate(redis_obj, user, num):
    print(f"{user}: CALCULATING {num}")
    summed = 0
    await redis_obj.publish(user, f'calculating sum...')
    for i in range(0, int(num)):
        await asyncio.sleep(3)
        summed += i
        print(f"{user}: FIRST STEP {summed}")
        await redis_obj.publish(user, f'calculate step. {summed}')
    await redis_obj.publish(user, f'finished calculate. {summed}')
    print(f"{user}: DONE {summed}")
    return summed


async def redis_stuff(redis_obj_in=None, pubsub_in=None):
    try:
        if redis_obj_in is None and pubsub_in is None:
            print(f'redis obj not provided. Manually connecting...')
            redis_obj, pubsub = await reconnect()
        else:
            print(f'redis obj provided.')
            redis_obj = redis_obj_in
            pubsub = pubsub_in
        with ProcessPoolExecutor(max_workers=10) as executor:
            print(f'listening to channel "ssm-channel"...')
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True)
                if message is not None:
                    msg_str = message['data'].decode()
                    msg_str = msg_str.replace("'", '"')
                    print(f'MESSAGE: {msg_str}')
                    data = json.loads(msg_str)
                    await redis_obj.publish(data['user'], f'data received.')
                    print(f"(Reader) Message Received: {message}")
                    if data['num'] == 'STOP':
                        print("(Reader) STOP")
                    executor.submit(calculate, redis_obj, data['user'], data['num'])
    except redis.ConnectionError:
        print(f'Pubsub lost connection!')
        try:
            new_obj, new_pubsub = await reconnect()
            await redis_stuff(new_obj, new_pubsub)
        except redis.ConnectionError:
            print(f'unable to recursively restart, quitting...')
            return
    # except Exception as e:
    #     template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    #     message = template.format(type(e).__name__, e.args)
    #     print(message)


async def reconnect():
    try:
        print(f'reconnecting...')
        redis_obj = redis.Redis()
        print(f'getting pubsub...')
        pubsub = redis_obj.pubsub()
        print(f'subscribing to pubsub...')
        await pubsub.psubscribe('ssm-channel')
    except redis.ConnectionError:
        print(f'Redis server unavailable. Retrying in 5 seconds...')
        await asyncio.sleep(5)
        redis_obj, pubsub = await reconnect()
    except Exception as e:
        print(f'uncaught error: {e.__traceback__}')
        return None, None
    return redis_obj, pubsub


if __name__ == "__main__":
    # asyncio.run(hello())
    asyncio.run(redis_stuff())
