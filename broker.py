
import zmq
import pickle
import time


# TYLER: Look in funcX worker for wrapping/unwrapping.
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


b_tasks = []
results = []

broker.poller.register(broker.client_socket, zmq.POLLIN)

while True:

    print("Getting new result")
    # Poll and get worker_id and result
    result = broker.poller.poll()

    worker_msg = None
    client_msg = None

    print("Pulling messages from worker...")
    try:
        worker_msg = broker.worker_socket.recv_multipart(flags=zmq.NOBLOCK)
        worker_result = pickle.loads(worker_msg[1])
        worker_command = pickle.loads(worker_msg[2])
    except zmq.ZMQError:
        print("No worker messages")
        pass

    print("Pulling messages from client...")
    try:
        client_msg = broker.client_socket.recv_multipart(flags=zmq.NOBLOCK)
        print(client_msg)
        task_id = pickle.loads(client_msg[1])
        client_tasks = pickle.loads(client_msg[2])
        client_command = pickle.loads(client_msg[3])

        for task in client_tasks:
            b_tasks.append(task)

    except zmq.ZMQError:
        print("No client messages")
        pass

    # If we have a message from worker, process it.
    if worker_msg is not None:

        task_id = pickle.loads(worker_msg[1])
        worker_result = pickle.loads(worker_msg[2])
        worker_command = pickle.loads(worker_msg[3])

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

        elif worker_command == "TASK_RETURN":

            print("GIVE EM BACK")
            # Receive from the worker.
            try:
                # worker_result = broker.worker_socket.recv_multipart(flags=zmq.NOBLOCK)
                print("RESULT RECEIVED")
                results.append(worker_result)
                print("RESULTS: {}".format(results))
            except zmq.ZMQError:
                print("Nothing to task_return.")
                pass

    if client_msg is not None:
        print("SENDING TASKS TO WORKER!")
        if len(b_tasks) > 0:
            print(b_tasks)
            for task in b_tasks:
                broker.worker_socket.send_multipart([b"A", task.encode()])
                print(task)
        else:
            print("NO TASKS")

    print("SENDING BACK RESULTS")
    if len(results) is not None:
        # Send to the client
        print("RESULTS LENGTH: {}".format(len(results)))
        for result in results:
            result.insert(0, b"B")
            print(result)
            broker.client_socket.send_multipart(result)
    else:
        print("NO RESULTS")

    # TODO: Send a bunch of zmq.NOBLOCK results back to client.
    time.sleep(0.5)
