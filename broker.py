
import zmq
import pickle
import time
import random
import logging
from queue import Queue


# TODO 1: Support having multiple container types connected simultaneously.
# TODO 2: Spin up containers automatically using subprocess (like process worker pool).
# TODO 3: Use actual containers with the workers (if container_reuse or container_single_use).
# TODO 4: Track resources on the system (can then use this for scheduling purposes).

# TODO: think of all workers as waiting for either WORK or KILL.

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


class WorkerPool(object):
    # k-v: work_type - task_q
    worker_types = {}

    # k-v: worker_id - socket_url
    workers = {}

    # k-v: worker_id - capacity (usually 0 or 1)
    worker_capacities = {}


class Broker(object):

    def __init__(self):

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

    def create_new_workers(self, w_type):
        port_range = range(50010, 54999)
        new_port = random.choice(port_range)
        print("CREATE")

    def kill_workers(self):
        print("KILL")


broker = Broker()
broker.worker_socket.bind("tcp://*:50001")
broker.client_socket.bind("tcp://*:50002")

worker_pool = WorkerPool()

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
        task_id = pickle.loads(client_msg[1])  # TODO: remove.

        b_tasks.append(client_msg)

        #client_tasks = pickle.loads(client_msg[2])
        #client_command = pickle.loads(client_msg[3])

        # # TODO: Dont' worry about picking apart batches yet.
        # for task in client_tasks:
        #     b_tasks.append((task_id, task))

            # {task_typeA: a_queue, task_typeB: b...}

    except zmq.ZMQError:
        print("No client messages")
        pass

    # If we have a message from worker, process it.
    if worker_msg is not None:

        # TODO: Read the first-n bytes instead.
        task_id = pickle.loads(worker_msg[1])
        worker_result = pickle.loads(worker_msg[2])
        worker_command = pickle.loads(worker_msg[3])

        # On registration, create worker and add to worker dicts.
        if worker_command == "REGISTER":

            # Add worker from its new port.
            worker_pool.workers[worker_result["wid"]] = "55555"  # TODO: Grab random port from spin-up.

            # Set our initial capacity to 1, meaning we're ready for a task.
            worker_pool.worker_capacities[worker_result["wid"]] = 1

            # If we don't have a queue for this type of work yet, create one!
            if worker_result["w_type"] not in worker_pool.worker_types:
                worker_pool.worker_types["w_type"] = Queue()

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
            for task in b_tasks:
                # Schema: worker_type, task_id, task_buffer (list).
                # TODO: un-hardcode the 'A'
                print(task)
                task[0] = b"A"
                broker.worker_socket.send_multipart(task)
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

    time.sleep(0.5)
