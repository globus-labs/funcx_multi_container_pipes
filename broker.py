
import zmq
import pickle
import time
import random
import logging
from queue import Queue
import subprocess
import uuid


# TODO 2: Spin up containers automatically using subprocess (like process worker pool).
# TODO 3: Use actual containers with the workers (if container_reuse or container_single_use).
# TODO 4: Track resources on the system (can then use this for scheduling purposes).

# TODO: think of all workers as waiting for either WORK or KILL.


class ZMQContext(object):
    def __init__(self):
        self.context = zmq.Context()
        self.poller = zmq.Poller()


class Client(object):
    def __init__(self, z_context):
        self.context = z_context.context
        self.poller = z_context.poller
        self.client_socket = self.context.socket(zmq.ROUTER)
        self.poller.register(self.client_socket, zmq.POLLIN)
        self.client_socket.bind("tcp://*:50002")
        self.client_identity = b"client_identity"

        print("Client socket successfully created!")

    def results_to_client(self, result):
        #context = zmq.Context.instance()
        #client = context.socket(zmq.ROUTER)
        #client.bind("tcp://*:50002")

        # Thread(target=worker_a).start()
        # Thread(target=worker_b).start()

        # Wait for threads to stabilize
        time.sleep(1)

        # Send 10 tasks scattered to A twice as often as B
        for _ in range(10):
            # Send two message parts, first the address…
            ident = random.choice([b'B', b'B', b'B'])
            # And then the workload
            work = b"This is the workload"
            self.client_socket.send_multipart([ident, pickle.dumps(result)])

        self.client_socket.send_multipart([b'B', pickle.dumps(result)])


class WorkerPool(object):

    def __init__(self, z_context):
        self.worker_types = {}  # k-v: work_type - task_q
        self.workers = {}  # k-v: worker_id - socket (ZMQ object)
        self.worker_capacities = {}  # k-v: worker_id - capacity (usually 0 or 1)

        # Do all the ZMQ stuff
        self.context = z_context.context
        self.poller = z_context.poller
        self.sockets_in_use = set()
        self.socket_range = range(50005, 59999)

        self.worker_socket = self.context.socket(zmq.ROUTER)
        self.worker_socket.linger = 0
        self.poller.register(self.worker_socket, zmq.POLLIN)
        self.sock_addr = 50010
        self.worker_socket.bind("tcp://*:{}".format(self.sock_addr))

    def create_worker(self, worker_type):
        wid = uuid.uuid4()

        # Keep trying until we get a non-conflicting socket_address.
        while True:
            port_addr = random.choice(self.socket_range)
            if port_addr not in self.sockets_in_use:
                self.sockets_in_use.add(port_addr)
                break
            else:
                print("Conflicting socket... Retry...")

        # Add to the queues
        if worker_type not in self.worker_types:
            self.worker_types[worker_type] = Queue()  # Create queue if there isn't already one for this worker type.

        #self.workers[wid] = worker_socket
        self.worker_capacities[wid] = 0

        # print("Successfully bound to worker socket at address {}".format(sock_addr))

        # Now actually spin it up.
        # cmd = "python3 worker.py -w {} -s {}".format(worker_type)
        cmd = "python3 worker.py -w {} -p {} -i {}".format(worker_type, port_addr, wid)
        #subprocess.Popen(cmd, shell=True)
        print("Successfully initialized worker! ")

        return wid


context = ZMQContext()

print("Creating client...")
client = Client(context)

# broker.worker_socket.bind("tcp://*:50001")

worker_pool = WorkerPool(context)


# TODO: We need a task queue for each type of work.
a_tasks = []
b_tasks = []
results = []

# worker_pool.create_worker('tabular-ext')


while True:

    print("Getting new result")
    # Poll and get worker_id and result
    result = context.poller.poll()

    worker_msg = None
    client_msg = None

    print("Pulling messages from worker...")
    # >>>>>>>>>>>>>>>>>>>>>>>>>>> Construction below this line >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    try:
        worker_msg = worker_pool.worker_socket.recv_multipart(flags=zmq.NOBLOCK)
        print(worker_msg)
        worker_result = pickle.loads(worker_msg[1])
        worker_command = pickle.loads(worker_msg[2])
    except zmq.ZMQError:
        print("No worker messages")
        pass

    print("Pulling messages from client...")

    work_type = b"OOOOOOOOOOPS"
    try:
        client_msg = client.client_socket.recv_multipart(flags=zmq.NOBLOCK)
        task_id = pickle.loads(client_msg[1])  # TODO: remove -- should parse bits instead.
        work_type = client_msg[-1]
        b_tasks.append(client_msg)

        print("Work type: {}".format(work_type.decode()))

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

            # Set our initial capacity to 1, meaning we're ready for a task.
            worker_pool.worker_capacities[worker_result["wid"]] = 1

            print("Successfully registered worker! ")

        elif worker_command == "TASK_RETURN":

            # Receive from the worker.
            try:
                # worker_result = broker.worker_socket.recv_multipart(flags=zmq.NOBLOCK)
                print("RESULT RECEIVED")
                results.append(worker_result)
                print("RESULTS: {}".format(results))
            except zmq.ZMQError:
                print("Nothing to task_return.")
                pass

    print("SENDING TASKS TO WORKER!")
    if len(b_tasks) > 0:
        for task in b_tasks:
            # Schema: worker_type, task_id, task_buffer (list).
            task[0] = work_type
            worker_pool.worker_socket.send_multipart(task)

    print("SENDING BACK RESULTS")
    if len(results) is not None:
        # Send to the client
        print("RESULTS LENGTH: {}".format(len(results)))
        for result in results:
            result.insert(0, client.client_identity)
            print(result)
            print("SENDING....")
            client.results_to_client(result)
            print("MESSAGE SUCCESSFULLY SENT!")
    else:
        print("NO RESULTS")

    time.sleep(0.5)
