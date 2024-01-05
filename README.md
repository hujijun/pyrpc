# pyrpc
<p align="center">
  <a href="https://circleci.com/gh/vibora-io/vibora"><img src="https://circleci.com/gh/vibora-io/vibora.svg?style=shield"></a>
  <a href="https://github.com/hujijun/pyrpc/blob/master/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/Apache-License-yellow.svg"></a>
  <a href="https://pypi.org/project/pyrpc/"><img alt="PyPI" src="https://img.shields.io/pypi/v/vibora.svg"></a>
  <a href="https://github.com/ambv/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>
python rpc lib

Server Example
--------------
```python
import asyncio
from pyrpc import RpcApp

rpc_app = RpcApp()

@rpc_app.route(1)
async def test1(a):
    return "test"+a

if __name__ == '__main__':
    asyncio.run(rpc_app.run_tcp_server("127.0.0.1", 8888))
```

Client Example
--------------

```python
import asyncio
from pyrpc import RpcClient


async def main():
    rpc_client = await RpcClient.create_client("localhost", 8888)
    a1 = rpc_client.async_exec(1, "a")
    a2 = rpc_client.async_exec(1, "b")
    print(await asyncio.gather(a1, a2))
    await rpc_client.close()

if __name__ == '__main__':
    asyncio.run(main())
```
