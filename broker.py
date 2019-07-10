
import zmq
import pickle


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

        self.client_socket = self.context.socket(zmq.REP)
        self.poller.register(self.client_socket, zmq.POLLIN)

    def connect_to_client(self):
        print("Hey")
        return "Sausage"

    def connect_to_worker(self):
        print("Hey2")
        return "Pepperoni"


broker = Broker()
broker.worker_socket.bind("tcp://*:50001")
broker.client_socket.bind("tcp://*:50005")


tasks = ["hello", "rello", "yello", "mello"]


poll_workers = zmq.Poller()
poll_workers.register(broker.worker_socket, zmq.POLLIN)

while True:

    print("Getting new result")

    # Poll and get worker_id and result
    result = broker.poller.poll()
    #req = broker.poller.poll()

    if result:
        print("Received result! ")
        msg = broker.worker_socket.recv_multipart()

        result = pickle.loads(msg[1])
        command = pickle.loads(msg[2])

        print(result)
        print(command)

        # On registration, create worker and add to worker dicts.
        # TODO: Can move this part out to the manager level.
        if command == "REGISTER":
            broker.workers[result["wid"]] = {
             "con_id": result["con_id"],
             "gpu_avail": result["gpu_avail"],
             "mem_avail": result["mem_avail"],
             "data_obj": result["data_obj"],
             "last_result": "REGISTER",
             "w_type": result["w_type"]
            }

            if result["w_type"] in broker.worker_types:
                broker.worker_types["w_type"].append(result["wid"])
            else:
                broker.worker_types["w_type"] = [result["wid"]]

            print("Successfully registered worker! ")

        # Catch returned tasks and return them to the client.
        elif command == "TASK_RETURN":  # TODO: Uncomment this when rigged to client.
            print("TASK TO RETURN.")

        for task in tasks:  # TODO: Receive 'tasks' (as batch) from the client.
            broker.worker_socket.send_multipart([b"A", task.encode()])  # TODO: Should THIS be async?


