
import zmq
import pickle

import time

# TODO: Look in funcX worker for wrapping/unwrapping.

class Worker(object):
    """a Worker, idle or active"""
    identity = None  # hex Identity of worker
    address = None  # Address to route to
    wtype = None

    def __init__(self, identity, address):
        self.identity = identity
        self.address = address
        self.w_type = None


class Broker(object):

    def __init__(self):
        self.worker_types = {}  # k-v == worker_type + list of id's
        self.workers = {}  # k-v == worker_id + resource reqs.

        self.context = zmq.Context()
        self.worker_socket = self.context.socket(zmq.ROUTER)
        self.worker_socket.linger = 0
        self.poller = zmq.Poller()
        self.poller.register(self.worker_socket, zmq.POLLIN)
        self.brokers = {}

        self.client_socket = self.context.socket(zmq.ROUTER)
        self.poller.register(self.client_socket, zmq.POLLIN)

    def connect_to_client(self):
        print("Hey")
        return "Sausage"

    def connect_to_worker(self):
        print("Hey2")
        return "Pepperoni"


broker = Broker()
broker.worker_socket.bind("tcp://*:50001")
broker.client_socket.bind("tcp://*:50002")


tasks = ["print('hello')", "print('mello')", "print('rello')", "print('yello')", "print('kello')"]
#results = queue.Queue()


#poll_workers = zmq.Poller()
broker.poller.register(broker.client_socket, zmq.POLLIN)

while True:

    print("Getting new result")

    # Poll and get worker_id and result
    result = broker.poller.poll()

    worker_msg = None
    client_msg = None

    try:
        worker_msg = broker.worker_socket.recv_multipart(flags=zmq.NOBLOCK)
        worker_result = pickle.loads(worker_msg[1])
        worker_command = pickle.loads(worker_msg[2])
    except zmq.ZMQError:
        print("No worker messages")
        pass

    try:
        client_msg = broker.client_socket.recv_multipart(flags=zmq.NOBLOCK)
        client_result = pickle.loads(client_msg[1])
        client_command = pickle.loads(client_msg[2])
    except zmq.ZMQError:
        print("No client messages")
        pass

    # If we have a message from worker, process it.
    if worker_msg is not None:

        worker_result = pickle.loads(worker_msg[1])
        worker_command = pickle.loads(worker_msg[2])

        # On registration, create worker and add to worker dicts.
        if worker_command == "REGISTER":
            broker.workers[worker_result["wid"]] = {
                "con_id": worker_result["con_id"],
                "gpu_avail": worker_result["gpu_avail"],
                "mem_avail": worker_result["mem_avail"],
                "data_obj": worker_result["data_obj"],
                "last_result": "REGISTER",
                "w_type": worker_result["w_type"]
            }

            if worker_result["w_type"] in broker.worker_types:
                broker.worker_types["w_type"].append(worker_result["wid"])
            else:
                broker.worker_types["w_type"] = [worker_result["wid"]]

            print("Successfully registered worker! ")


        #
        # # Catch returned tasks and return them to the client.
        # elif command == "TASK_RETURN":  # TODO: Uncomment this when rigged to client.
        #     print("TASK TO RETURN.")
        #
        if worker_command == "TASK_RETURN":
            print("RETURNING!")

        if tasks is not None:
            for task in tasks:  # TODO: Receive 'tasks' (as batch) from the client.
                broker.worker_socket.send_multipart([b"A", task.encode()])  # TODO: Should THIS be async?async

    print("MADE IT HERE.")

    print(len(broker.workers))

    time.sleep(0.5)
