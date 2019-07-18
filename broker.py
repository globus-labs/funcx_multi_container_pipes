
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

        print("Client socket successfully created!")


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
        # while True:
        #     sock_addr = random.choice(self.socket_range)
        #     if sock_addr not in self.sockets_in_use:
        #         self.sockets_in_use.add(sock_addr)
        #         break
        #     else:
        #         print("Conflicting socket... Retry...")


        # Add to the queues
        if worker_type not in self.worker_types:
            self.worker_types[worker_type] = Queue()  # Create queue if there isn't already one for this worker type.

        #self.workers[wid] = worker_socket
        self.worker_capacities[wid] = 0

        # print("Successfully bound to worker socket at address {}".format(sock_addr))

        # Now actually spin it up.
        # cmd = "python3 worker.py -w {} -s {}".format(worker_type)
        cmd = "python3 worker.py -w {}".format(worker_type)
        #subprocess.Popen(cmd, shell=True)
        print("Successfully initialized worker! ")

        return wid


context = ZMQContext()

print("Creating client...")
client = Client(context)

# broker.worker_socket.bind("tcp://*:50001")

worker_pool = WorkerPool(context)

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
    #for wid in worker_pool.workers:  #.workers is a socket object
    try:
        worker_msg = worker_pool.worker_socket.recv_multipart(flags=zmq.NOBLOCK)
        # worker_msg = worker_pool.worker_socket.recv_multipart()
        print(worker_msg)
        worker_result = pickle.loads(worker_msg[1])
        worker_command = pickle.loads(worker_msg[2])
        #break  # Break if we get a message from one of our sockets
    except zmq.ZMQError:
        print("No worker messages")
        pass

    #print(worker_msg)

    print("Pulling messages from client...")
    try:
        client_msg = client.client_socket.recv_multipart(flags=zmq.NOBLOCK)
        print(client_msg)
        task_id = pickle.loads(client_msg[1])  # TODO: remove.

        b_tasks.append(client_msg)

        # client_tasks = pickle.loads(client_msg[2])
        # client_command = pickle.loads(client_msg[3])

        # # TODO: Dont' worry about picking apart batches yet.
        # for task in client_tasks:
        #     b_tasks.append((task_id, task))

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

    # TODO: We probably don't need the 'is not None' part??? We want to send tasks regardless.
    if client_msg is not None:
        print("SENDING TASKS TO WORKER!")
        if len(b_tasks) > 0:
            for task in b_tasks:
                # Schema: worker_type, task_id, task_buffer (list).
                # TODO: un-hardcode the 'A'
                print(task)
                task[0] = b"C"
                #broker.worker_socket.send_multipart(task)  # TODO: bring this back
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
            client.client_socket.send_multipart(result)
    else:
        print("NO RESULTS")

    time.sleep(0.5)
