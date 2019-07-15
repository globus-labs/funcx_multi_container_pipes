
import zmq
import pickle
import random
import uuid

context = zmq.Context()
broker_socket = context.socket(zmq.DEALER)
broker_socket.setsockopt(zmq.IDENTITY, "B".encode())

broker_socket.connect("tcp://localhost:50002")

worker_types = ["A", "B", "C"]
tasks = ["print('hello')", "print('hello2')",  "print('hello3')",  "print('hello4')",  "print('hello5')"]

for req in range(1):

    print("Sending request {}".format(req))
    # Schema: task_id, task_buffer (list), 'command', 'w_type']
    broker_socket.send_multipart([pickle.dumps(uuid.uuid4), pickle.dumps(tasks), pickle.dumps("INVOKE"), pickle.dumps(random.choice(worker_types))])

    message = broker_socket.recv_multipart()
