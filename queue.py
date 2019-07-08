
import zmq
import pickle


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
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.linger = 0
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.brokers = {}


broker = Broker()
broker.socket.bind("tcp://*:50001")

tasks = []


poll_workers = zmq.Poller()
poll_workers.register(broker.socket, zmq.POLLIN)


while True:

    print("Getting new result")

    # Poll and get worker_id and result
    result = broker.poller.poll()

    if result:
        print("Received result! ")
        msg = broker.socket.recv_multipart()

        result = pickle.loads(msg[1])
        command = pickle.loads(msg[2])

        # On registration, create worker and add to worker dicts.
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

    #### CONSTRUCTION ZONE ####
    # TODO: This isn't actually receiving back any message.
    print("Sending back to worker")
    broker.socket.send_multipart([pickle.dumps("hello")])
    print("Successfully sent!")
    ###########################

        # elif command == "TASK":
        #     for task in tasks:
        #         broker.socket.send_multipart(broker.workers)

        # else:
        #     print("ERROR~!!!")

        # print(broker.workers)
