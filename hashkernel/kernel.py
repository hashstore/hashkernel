import json
import sys
from datetime import datetime

from hashkernel import CodeEnum, ensure_module
from hashkernel.bakery import Cake
from hashkernel.logic import HashLogic
from hashkernel.smattr import SmAttr


class MsgType(CodeEnum):
    QUERY = (0,)
    DATA = (1,)
    CALL = (2,)
    ASK = (3,)


class Header(SmAttr):
    type: MsgType
    src: Cake
    dest: Cake
    time: datetime


class EndPoint:
    def __init__(self, end_point=None, host="127.0.0.1"):
        self.session_id = Cake.new_portal()
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.PAIR)
        if end_point is None:
            port_selected = self.socket.bind_to_random_port(
                f"tcp://{host}", min_port=5678, max_port=32124, max_tries=100
            )
            self.end_point = f"tcp://{host}:{port_selected}"
        else:
            self.socket.bind(end_point)
            self.end_point = end_point


class Kernel:
    def __init__(self, logic, session_id, end_point):
        self.logic = HashLogic.from_module(ensure_module(logic))
        self.session_id = session_id
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.PAIR)
        self.socket.connect(end_point)

    # def loop(self):
    #     self.socket.recv()
    #     self.logic


def start_reactor(logic):
    config = json.loads(sys.stdin.read())
    Kernel(logic, config["session_id"], config["end_point"]).loop()
