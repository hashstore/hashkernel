# #!/usr/bin/env python
# # -*- coding: utf-8 -*-
# from __future__ import unicode_literals
#
# import logging
# import os
# import signal
# import sys
# import time
#
# from hashkernel import ensure_bytes, exception_message, json_encode
# from hashkernel.bakery import Content, NotAuthorizedError
# from hashkernel.file_types import file_types
#
# JSON__MIME = file_types["JSON"].mime
#
# log = logging.getLogger(__name__)
#
#
# def stop_server(signum, frame):
#     ioloop = tornado.ioloop.IOLoop.current()
#     ioloop.add_callback(ioloop.stop)
#     print("Stopped!")
#
#
# class _ContentHandler(tornado.web.RequestHandler):
#     SUPPORTED_METHODS = ["GET"]
#
#     @gen.coroutine
#     def get(self, path):
#         try:
#             content = self.content(path)
#             self.set_header("Content-Type", content.mime)
#             if content.has_file() and os.name != "nt":
#                 self.stream = PipeIOStream(content.open_fd())
#                 self.stream.read_until_close(
#                     callback=self.on_file_end, streaming_callback=self.on_chunk
#                 )
#             else:
#                 self.finish(content.get_data())
#         except NotAuthorizedError:
#             self.write(exception_message())
#             self.send_error(403)
#         except FileNotFoundError:
#             self.send_error(404)
#         except:
#             logging.exception("error")
#             self.send_error(500)
#
#     def on_file_end(self, s):
#         if s:
#             self.write(s)
#         self.finish()  # close connection
#
#     def on_chunk(self, chunk):
#         self.write(chunk)
#         self.flush()
#
#
# class PoolHandler(tornado.web.RequestHandler):
#     SUPPORTED_METHODS = ["POST"]
#
#     @gen.coroutine
#     def post(self, kernel_id):
#         try:
#             self.set_header("Content-Type", JSON__MIME)
#             if content.has_file() and os.name != "nt":
#                 self.stream = PipeIOStream(content.open_fd())
#                 self.stream.read_until_close(
#                     callback=self.on_file_end, streaming_callback=self.on_chunk
#                 )
#             else:
#                 self.finish(content.get_data())
#         except NotAuthorizedError:
#             self.write(exception_message())
#             self.send_error(403)
#         except FileNotFoundError:
#             self.send_error(404)
#         except:
#             logging.exception("error")
#             self.send_error(500)
#
#     def on_file_end(self, s):
#         if s:
#             self.write(s)
#         self.finish()  # close connection
#
#     def on_chunk(self, chunk):
#         self.write(chunk)
#         self.flush()
#
#
# def _string_handler(s):
#     class StringHandler(_ContentHandler):
#         def content(self, _):
#             return Content(data=ensure_bytes(s), mime="text/plain")
#
#     return StringHandler
#
#
# class LoopServer:
#     def __init__(self, port):
#         self.port = port
#         self.io_loop = tornado.ioloop.IOLoop.current()
#
#     async def get_pid(self):
#         http_client = tornado.httpclient.AsyncHTTPClient()
#         try:
#             response = await http_client.fetch(f"http://localhost:{self.port}/-/pid")
#         except Exception as e:
#             logging.warning(f"Error: {e}")
#             raise
#         else:
#             return int(response.body)
#
#     def shutdown(self, wait_until_down):
#         try:
#             while True:
#                 pid = self.io_loop.run_sync(self.get_pid)
#                 print(pid)
#                 if pid:
#                     logging.warning("Stopping %d" % pid)
#                     os.kill(pid, signal.SIGINT)
#                     if wait_until_down:
#                         time.sleep(2)
#                     else:
#                         break
#                 else:
#                     break
#         except Exception as e:
#             print(f"{e}")
#
#     def run_server(self):
#         pid = str(os.getpid())
#         print(f"pid: {pid}")
#
#         handlers = [
#             (r"/-/(pid)$", _string_handler(pid)),
#             (r"/-/pool/(.*)$", _string_handler(pid)),
#             (r"(.*)$", _string_handler("index"))
#             # - ~ _
#         ]
#         application = tornado.web.Application(handlers)
#         signal.signal(signal.SIGINT, stop_server)
#         http_server = tornado.httpserver.HTTPServer(application, max_body_size=222222)
#         http_server.listen(self.port)
#         print(f"LoopServer: listening=0.0.0.0:{self.port}")
#         tornado.ioloop.IOLoop.current().start()
#         print("Finished")
#
#
# if __name__ == "__main__":
#     server = LoopServer(7654)
#     if len(sys.argv) > 1 and sys.argv[1] == "stop":
#         server.shutdown(True)
#     else:
#         server.run_server()
