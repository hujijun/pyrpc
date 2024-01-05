import asyncio
import logging
from pyrpc import RpcApp

rpc_app = RpcApp()

@rpc_app.route(1)
async def test1(a):
    logging.info(a)
    await asyncio.sleep(3)
    return "ssa"+a

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s - %(filename)s:%(lineno)d %(message)s')
    asyncio.run(rpc_app.run_tcp_server("127.0.0.1", 8888))