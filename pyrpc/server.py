import asyncio
import logging
from asyncio import streams
from types import coroutine
import msgpack

from pyrpc.base import RpcBase, ResponseStatus, SocketCloseException, RpcRequest, RpcResponse


class RpcConnection(RpcBase):
    router_mapping = {}

    async def get_request(self) -> RpcRequest:
        """获取请求"""
        channel_id, method_id, page_size = await self.recv_header()
        args = msgpack.unpackb(await self.recv_size(page_size), use_list=False, raw=False) if page_size else ()
        logging.debug(f"Recv channel_id: {channel_id}, method_id: {method_id}, page_size: {page_size}, args: {args}")
        return RpcRequest(channel_id, method_id, args)

    async def send_response(self, response_coroutine: coroutine):
        """发送响应"""
        response: RpcResponse = await response_coroutine
        self.send_frame(response.channel_id, response.status.value, response.body)


class RpcApp(object):

    def __init__(self, routers:list = None):
        self.routers = routers or {}

    def add_route(self, method_id:int, method: callable):
        self.routers[method_id] = method

    def route(self, method_id:int):
        def decorator(func):
            self.add_route(method_id, func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator

    async def dispatch(self, request: RpcRequest) -> RpcResponse:
        try:
            r = await self.routers[request.method_id](*request.args)
            status = ResponseStatus.success
            body = r or b""
        except KeyError:
            status = ResponseStatus.fail
            body = b"Invalid method"
        except Exception as e:
            status = ResponseStatus.fail
            body = str(e).encode()
        return RpcResponse(request.channel_id, status, body)

    async def handle(self, reader: streams.StreamReader, writer: streams.StreamWriter):
        ip, port = writer.get_extra_info('peername')
        logging.info(f"Access {ip}:{port}")
        connection = RpcConnection(reader, writer)
        while not connection.closed:
            try:
                request = await connection.get_request()
                response_coroutine = self.dispatch(request)
                asyncio.create_task(connection.send_response(response_coroutine))
            except SocketCloseException:
                break
            except Exception as error:
                logging.exception(error)
                continue
        logging.info("Close the connection")
        await connection.close()

    async def run_tcp_server(self, ip: str, port: int):
        server = await asyncio.start_server(self.handle, ip, port)
        async with server:
            logging.info(f'Serving on {ip}:{port}')
            await server.serve_forever()
