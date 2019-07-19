
import zmq
import pickle
import time
import random
import logging
from queue import PriorityQueue
import queue
import subprocess
import uuid


# TODO 2: Spin up containers automatically using subprocess (like process worker pool).
# TODO 3: Use actual containers with the workers (if container_reuse or container_single_use).
# TODO 4: Track resources on the system (can then use this for scheduling purposes).

# TODO: think of all workers as waiting for either WORK or KILL.

from dataclasses import dataclass, field
from typing import Any


@dataclass(order=True)
class PrioritizedItem:
    priority: int
    item: Any = field(compare=False)


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
        self.client_socket.bind("tcp://*:50052")
        self.client_identity = b"client_identity"

        print("Client socket successfully created!")

    def results_to_client(self, result):

        # Wait for threads to stabilize
        time.sleep(1)

        # Send 10 tasks scattered to A twice as often as B
        # TODO: THIS RETURNS 10 TIMES!!!
        for _ in range(1):
            # Send two message parts, first the address…
            ident = random.choice([b'B', b'B', b'B'])
            # And then the workload
            work = b"This is the workload"
            self.client_socket.send_multipart([ident, pickle.dumps(result)])

        self.client_socket.send_multipart([b'B', pickle.dumps(result)])


class WorkerPool(object):

    def __init__(self, z_context):
        self.worker_types = {b'A': PriorityQueue(), b'B': PriorityQueue()}  # k-v: work_type - task_q (PriorityQueue)
        self.workers = {}  # k-v: worker_id - socket (ZMQ object)
        self.work_capacities = {}  # k-v: work_type - capacity (integer)

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
            self.worker_types[worker_type] = PriorityQueue()  # Create queue if not one for this worker type.
        if worker_type not in self.work_capacities:
            self.work_capacities[worker_type] = 0

        # Now actually spin it up.
        # cmd = "python3 worker.py -w {} -s {}".format(worker_type)
        cmd = "python3 worker.py -w {} -p {} -i {}".format(worker_type, port_addr, wid)
        #subprocess.Popen(cmd, shell=True)
        print("Successfully initialized worker! ")

        return wid

    def recv_client_message(self, cli):
        try:
            client_msg = cli.client_socket.recv_multipart(flags=zmq.NOBLOCK)
            task_id = pickle.loads(client_msg[1])  # TODO: remove -- should parse bits instead.
            work_type = client_msg[-1]

            pri_queue = self.worker_types[work_type]

            pri_queue.put(PrioritizedItem(5, client_msg))
            print("Priority Queue size: {}".format(pri_queue.qsize()))

            print("Work type: {}".format(work_type.decode()))

        except zmq.ZMQError:
            print("No client messages")
            pass

    def send_results_to_client(self, cli, res_q):
        while not results.empty():

            result = res_q.get()
            result.insert(0, cli.client_identity)
            print(result)
            print("SENDING....")
            cli.results_to_client(result)
            print("MESSAGE SUCCESSFULLY SENT!")
        print("ALL CURRENT RESULTS RETURNED. ")

    def populate_results(self):
        worker_task_type = worker_msg[4]  # Parse out the worker_type.
        print("WORK TYPE: {}".format(worker_task_type))

        self.work_capacities[worker_task_type.decode()] += 1  # Add 1 back to the capacity.

        # Receive from the worker.
        try:
            # worker_result = broker.worker_socket.recv_multipart(flags=zmq.NOBLOCK)
            print("RESULT RECEIVED")
            results.put(worker_result)
            print("RESULTS: {}".format(results))
        except zmq.ZMQError:
            print("Nothing to task_return.")
            pass

    def register_worker(self, reg_message):
        # Set our initial capacity to 1, meaning we're ready for a task.
        if reg_message["w_type"] in self.work_capacities:
            self.work_capacities[reg_message["w_type"]] += 1
        else:
            self.work_capacities[reg_message["w_type"]] = 5

        print("Successfully registered worker! ")

    def send_results_to_worker(self):
        print("HEY")

    def kill_worker(self, identity):
        # TODO: Find the worker with the lowest queue
        # TODO: Wrap up this stuff.
        print("KILL")


context = ZMQContext()

print("Creating client...")
client = Client(context)

worker_pool = WorkerPool(context)


results = queue.Queue()

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

    work_type = b"*******"

    # Check to see if client message exists and add to appropriate queues.
    worker_pool.recv_client_message(client)

    # If we have a message from worker, process it.
    if worker_msg is not None:

        # TODO: Read the first-n bytes instead.
        task_id = pickle.loads(worker_msg[1])
        worker_result = pickle.loads(worker_msg[2])
        worker_command = pickle.loads(worker_msg[3])
        task_type = worker_msg[4]

        # On registration, create worker and add to worker dicts.
        if worker_command == "REGISTER":
            worker_pool.register_worker(reg_message=worker_result)

        elif worker_command == "TASK_RETURN":
            worker_pool.send_results_to_client(client, results)

        else:
            raise NameError("[funcX] Unknown command type.")


    print(worker_pool.work_capacities)

    # TODO: SWITCH entire model to hand task to individual works instead
    #           (FIFO workers' work_request queue -- rather than capacity?)

    for work_type in worker_pool.work_capacities:
        print("Available worker capacity for {}: {}".format(work_type, worker_pool.work_capacities[work_type]))
        for _ in range(worker_pool.work_capacities[work_type]):  # Get one message for each available capacity.

            print(worker_pool.worker_types[work_type.encode()].qsize())
            job_data = None

            if worker_pool.worker_types[work_type.encode()].qsize() > 0:
                job_data = worker_pool.worker_types[work_type.encode()].get()
            print("JOB DATA: {}".format(job_data))

            print(job_data)
            if job_data is not None:
                print("INSIDE JOB_DATA")
                job_data.item[0] = work_type.encode()
                worker_pool.worker_socket.send_multipart(job_data.item)
                worker_pool.work_capacities[work_type] -= 1

    print("SENDING BACK RESULTS")
    worker_pool.send_results_to_client(client, results)

    time.sleep(0.5)
