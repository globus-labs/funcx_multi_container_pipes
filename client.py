
import zmq
import pickle
import random
import uuid

from ipyparallel.serialize import serialize_object, unpack_apply_message, pack_apply_message

context = zmq.Context()
broker_socket = context.socket(zmq.DEALER)
broker_socket.setsockopt(zmq.IDENTITY, "B".encode())

broker_socket.connect("tcp://localhost:50002")

worker_types = ["A", "B", "C"]
# TODO: ^^^ check the first few bytes task_type (uuid is 16 bytes)


def add2nums(num1, num2):
    return num1 + num2


for req in range(1):
    print("Sending request {}".format(req))
    task_id = str(uuid.uuid4())

    args = {"num1": 2, "num2": 2}
    kwargs = {}

    obj = pack_apply_message(add2nums, args, kwargs)


    # print(task_id)
    # TODO: Add back container job type sorting.
    # Schema: task_id, task_buffer (list), 'command', 'w_type']
    broker_socket.send_multipart([pickle.dumps(task_id), pickle.dumps(obj), pickle.dumps("INVOKE"), b'A'])

    while True:
        message = broker_socket.recv_multipart()
        print(message)
