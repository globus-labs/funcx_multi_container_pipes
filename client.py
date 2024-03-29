
import zmq
import pickle
import random
import uuid
import time

from ipyparallel.serialize import serialize_object, unpack_apply_message, pack_apply_message


context = zmq.Context()
worker = context.socket(zmq.DEALER)
worker.setsockopt(zmq.IDENTITY, b"client_identity")
worker.connect("tcp://127.0.0.1:50052")


def add2nums(num1, num2):
    return num1 + num2


for req in range(10):

    # time.sleep(4)
    print("Sending request {}".format(req))
    task_id = str(uuid.uuid4())

    args = {"num1": 2, "num2": 2}
    kwargs = {}

    obj = pack_apply_message(add2nums, args, kwargs)
    worker.send_multipart([pickle.dumps(task_id), pickle.dumps(obj), pickle.dumps("INVOKE"), b'B'])
    worker.send_multipart([pickle.dumps(task_id), pickle.dumps(obj), pickle.dumps("INVOKE"), b'A'])


total = 0
while True:

    print("Listening for new result")
    # We receive one part, with the workload
    request = worker.recv_multipart()

    # time.sleep(4)

    print(request)
