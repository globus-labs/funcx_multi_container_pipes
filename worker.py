
import zmq
import uuid
import pickle
import logging
from zhelpers import dump
import MDP
import time

class Worker:
    def __init__(self):
        self.wid = uuid.uuid4()
        self.con_id = None
        self.gpu_avail = None
        self.mem_avail = None
        self.data_obj = None

        self.service = "echo"

        self.broker_path = "tcp://localhost:50001"
        self.context = zmq.Context()
        self.poller = zmq.Poller()

        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect(self.broker_path)
        self.poller.register(self.socket, zmq.POLLIN)

    # def recv(self, reply=None, service=None):
    #     """Send reply, if any, to broker and wait for next request."""
    #     # Format and send the reply if we were provided one
    #      # assert reply is not None or not self.expect_reply
    #
    #     # TODO: Might want to do something here to separate send and receive function (see endpoint code)
    #     # TODO: Use the send function to advertise task capacity
    #
    #     # if reply is not None:
    #     #     assert self.reply_to is not None
    #     #     reply = [self.reply_to, b''] + reply
    #     #     print("sending reply {}".format(reply))
    #     #     self.send_to_broker(MDP.W_REPLY, msg=reply)
    #
    #
    #     while True:
    #         # Poll socket for a reply, with timeout
    #         try:
    #             items = self.poller.poll(self.timeout)
    #         except KeyboardInterrupt:
    #             break # Interrupted
    #
    #         if items:
    #             msg = self.broker.recv_multipart()
    #             print("Received {} at service {}".format(msg, service))
    #             assert len(msg) >= 3
    #
    #             empty = msg.pop(0)
    #             assert empty == b''
    #
    #             header = msg.pop(0)
    #             assert header == MDP.W_WORKER
    #
    #             command = msg.pop(0)
    #             if command == MDP.W_REQUEST:
    #                 # We should pop and save as many addresses as there are
    #                 # up to a null part, but for now, just save one…
    #                 self.reply_to = msg.pop(0)
    #                 # pop empty
    #                 empty = msg.pop(0)
    #                 assert empty == b''
    #
    #                 return msg # We have a request to process
    #
    #             else :
    #                 logging.error("E: invalid input message: ")
    #                 dump(msg)
    #
    #     return None


worker = Worker()
result = {"wid": worker.wid,
          "con_id": worker.con_id,
          "gpu_avail": worker.gpu_avail,
          "mem_avail": worker.mem_avail,
          "data_obj": worker.data_obj,
          "result": "REGISTER",
          "w_type": 1}

#logging.info("Connecting to broker...")
print("Registering worker...")
# worker.broker.send_multipart([pickle.dumps(result), pickle.dumps("REGISTER")])


while True:
    print("Polling for items...")
    items = worker.socket.poll()

    print("Found items!")
    print(items)
    if items:
        msg = worker.socket.recv_multipart()
        break  # Worker was interrupted
