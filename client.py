
import zmq
import pickle
import random
import uuid

from ipyparallel.serialize import serialize_object, unpack_apply_message, pack_apply_message

# context = zmq.Context()
# broker_socket = context.socket(zmq.DEALER)
# broker_socket.setsockopt(zmq.IDENTITY, b'B')
#
# broker_socket.connect('tcp://127.0.0.1:59999')
#
# worker_types = ["A", "B", "C"]
# # TODO: ^^^ check the first few bytes task_type (uuid is 16 bytes)
#
#

#
#     # TODO: Add back container job type sorting.
    # Schema: task_id, task_buffer (list), 'command', 'w_type']
    #broker_socket.send_multipart([pickle.dumps(task_id), pickle.dumps(obj), pickle.dumps("INVOKE"), b'B'])

#def worker_b(context=None):

# context = context or zmq.Context.instance()
context = zmq.Context()
worker = context.socket(zmq.DEALER)
worker.setsockopt(zmq.IDENTITY, b'B')
worker.connect("tcp://127.0.0.1:50002")


def add2nums(num1, num2):
    return num1 + num2


for req in range(20):
    print("Sending request {}".format(req))
    task_id = str(uuid.uuid4())

    args = {"num1": 2, "num2": 2}
    kwargs = {}

    obj = pack_apply_message(add2nums, args, kwargs)
    worker.send_multipart([pickle.dumps(task_id), pickle.dumps(obj), pickle.dumps("INVOKE"), b'B'])



total = 0
while True:
    # We receive one part, with the workload
    request = worker.recv()
    print(request)
    break


# worker_b()