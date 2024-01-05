import asyncio
import logging
from pyrpc import RpcClient


async def main():
    rpc_client = await RpcClient.create_client("localhost", 8888)
    a1 = rpc_client.async_exec(1, "babweasdfwed")
    a2 = rpc_client.async_exec(1, "aa")
    try:
        a, b = await asyncio.gather(a1, a2)
        logging.info(a)
        logging.info(b)
    except Exception as e:
        logging.error(e)
    bb = await rpc_client.async_exec(1, "we")
    print(bb)
    await rpc_client.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s - %(filename)s:%(lineno)d %(message)s')
    asyncio.run(main())
