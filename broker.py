
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
        for _ in range(10):
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

    def kill_worker(self, identity):
        # TODO: Find the worker with the lowest queue
        # TODO: Wrap up this stuff.
        print("KILL")

    def select_new_task(self, worker_type):
        # Access the appropriate task queue.
        return self.worker_types[worker_type].pop()


context = ZMQContext()

print("Creating client...")
client = Client(context)

worker_pool = WorkerPool(context)

# TODO: We need a task queue for each type of work.

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
    try:
        client_msg = client.client_socket.recv_multipart(flags=zmq.NOBLOCK)
        task_id = pickle.loads(client_msg[1])  # TODO: remove -- should parse bits instead.
        work_type = client_msg[-1]

        # TODO: Append the task to an appropriate queue
        pri_queue = worker_pool.worker_types[work_type]

        worker_pool.worker_types[work_type].put(PrioritizedItem(5, client_msg))
        print(worker_pool.worker_types[work_type].qsize())

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

            print(worker_result)

            # Set our initial capacity to 1, meaning we're ready for a task.
            if worker_result["w_type"] in worker_pool.work_capacities:
                worker_pool.work_capacities[worker_result["w_type"]] += 1
            else:
                worker_pool.work_capacities[worker_result["w_type"]] = 1

            print("Successfully registered worker! ")

        elif worker_command == "TASK_RETURN":
            # TODO: Also adjust the capacity of each worker to +1.  FREAKIN DO THIS, TYLER.

            # Receive from the worker.
            try:
                # worker_result = broker.worker_socket.recv_multipart(flags=zmq.NOBLOCK)
                print("RESULT RECEIVED")
                results.put(worker_result)
                print("RESULTS: {}".format(results))
            except zmq.ZMQError:
                print("Nothing to task_return.")
                pass

    print("SENDING TASKS TO WORKER!")

    # TODO: Change how we access these.
    # if len(b_tasks) > 0:
    #     for task in b_tasks:
    #         # Schema: worker_type, task_id, task_buffer (list).
    #         task[0] = work_type
    #         worker_pool.worker_socket.send_multipart(task)

    # TODO: Iterate over all worker available-capacities (n).

    print("IN THE CAPACITY LOOP")
    print(worker_pool.work_capacities)
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
    while not results.empty():

        result = results.get()
        # # Send to the client
        # print("RESULTS LENGTH: {}".format(len(results)))
        #for result in results:
        result.insert(0, client.client_identity)
        print(result)
        print("SENDING....")
        client.results_to_client(result)
        print("MESSAGE SUCCESSFULLY SENT!")
    else:
        print("NO RESULTS")

    time.sleep(0.5)
