
import zmq
import uuid
import time
import queue
import pickle
import subprocess


from queue import PriorityQueue

# TODO 2: Spin up containers automatically using subprocess (like process worker pool).
# TODO 3: Use actual containers with the workers (if container_reuse or container_single_use).
# TODO 4: Track resources on the system (can then use this for scheduling purposes).

# TODO: think of all workers as waiting for either WORK or KILL.

# TODO: Killing and starting workers in no_reuse mode should happen back-to-back


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

    def results_to_client(self, res):
        print("Sending results back to client...")
        self.client_socket.send_multipart([self.client_identity, pickle.dumps(res)])
        print("Successfully sent results!")

        # self.client_socket.send_multipart([b'B', pickle.dumps(res)])  # TODO: Unhardcode.


class Worker(object):
    def __init__(self, worker_type, port_addr, container_uri=None, container_mode=None):
        self.capacity = 0
        self.container_mode = container_mode
        self.container_uri = container_uri
        self.port_addr = port_addr
        self.task_q = queue.Queue()  # This will be the local queue of each worker.
        self.wid = uuid.uuid4()
        self.worker_type = worker_type

    def launch(self):
        print("Launching new worker of container_mode: {}".format(self.container_mode))

        if self.container_mode is None:
            cmd = "python3 worker.py -w {} -p {} -i {}".format(self.worker_type, self.port_addr, self.wid)
        elif self.container_mode == "singularity_reuse":
            cmd = "singularity run ..."
        elif self.container_mode == "no_reuse":
            cmd = "singularity run ...".format(self.container_uri)
        else:
            raise NameError("Invalid container launch mode.")

        exit_code = subprocess.Popen(cmd, shell=True)
        return exit_code

    def kill(self):
        print("Killing worker")


class WorkerPool(object):

    def __init__(self, z_context):

        self.task_queues = {b'A': PriorityQueue(), b'B': PriorityQueue()}  # k-v: task_type - task_q (PriorityQueue)

        # TODO: Change this dict to be k-v of worker_id -> worker.
        self.worker_capacities = {}  # k-v: worker_id - capacity (integer... should only ever be 0 or 1).
        self.task_to_worker_sets = {}  # k-v: task_type - workers (set)

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
        self.dead_worker_set = set()

    def create_worker(self, worker_type, container=None):

        worker = Worker(worker_type, self.sock_addr, container_uri=None, container_mode=None)  # TODO: container_* shouldn't be hardcoded.
        wid = worker.wid

        # TODO: Move this outside of this function.
        # Keep trying until we get a non-conflicting socket_address.
        # while True:
        #     port_addr = random.choice(self.socket_range)
        #     if port_addr not in self.sockets_in_use:
        #         self.sockets_in_use.add(port_addr)
        #         break
        #     else:
        #         print("Conflicting socket... Retry...")

        # Add to the queues
        if worker_type not in self.task_queues:
            self.task_queues[worker_type] = PriorityQueue()  # Create queue if not one for this worker type.
        if worker_type not in self.worker_capacities:  # Init wid's capacity to be zero until we finish setting it up.
            self.worker_capacities[worker_type] = 0
        if worker_type not in self.task_to_worker_sets:
            self.task_to_worker_sets[worker_type] = set()
            self.task_to_worker_sets[worker_type].add(wid)
        else:
            self.task_to_worker_sets[worker_type].add(wid)

        print(self.task_queues)
        print(self.worker_capacities)
        print(self.task_to_worker_sets)

        # TODO: >>> Connect to worker 'launch' function.  (Remove when it works).
        worker.launch()

        print("Successfully initialized worker! ")
        return "DONE"

    def recv_client_message(self, cli):
        try:
            client_msg = cli.client_socket.recv_multipart(flags=zmq.NOBLOCK)
            task_id = pickle.loads(client_msg[1])  # TODO: remove -- should parse bits instead.
            work_type = client_msg[-1]

            pri_queue = self.task_queues[work_type]

            pri_queue.put(PrioritizedItem(5, client_msg))
            print("Priority Queue size: {}".format(pri_queue.qsize()))

            print("Work type: {}".format(work_type.decode()))

        except zmq.ZMQError:
            print("No client messages")

    def send_results_to_client(self, cli, res_q):

        print("Checking length of results...")
        while not results.empty():

            print("Length of results is greater than zero!!!")
            result = res_q.get()
            print("RESULT: {}".format(result))
            result.insert(0, cli.client_identity)
            print("SENDING RESULTS BACK TO CLIENT....")
            cli.results_to_client(result)
            print("MESSAGE SUCCESSFULLY SENT!")
        print("ALL CURRENT RESULTS RETURNED. ")

    def populate_results(self, worker_result):
        worker_task_type = worker_msg[4]  # Parse out the worker_type.  # TODO.
        print("WORK TYPE: {}".format(worker_task_type))

        self.worker_capacities[worker_task_type.decode()] += 1  # Add 1 back to the capacity.

        # Receive from the worker.
        try:
            print("RESULT RECEIVED")
            results.put(worker_result)
            print("RESULTS: {}".format(results))
        except zmq.ZMQError:
            print("Nothing to task_return.")
            pass

    def register_worker(self, reg_message):
        # Set our initial capacity to 1, meaning we're ready for a task.
        if reg_message["w_type"] in self.worker_capacities:
            self.worker_capacities[reg_message["w_type"]] += 1
        else:
            self.worker_capacities[reg_message["w_type"]] = 5  # TODO.

        print("Successfully registered worker! ")

    def update_worker_capacities(self):
        for work_type in self.worker_capacities:
            print("Available worker capacity for {}: {}".format(work_type, self.worker_capacities[work_type]))
            for _ in range(self.worker_capacities[work_type]):  # Get one message for each available capacity.

                print(self.task_queues[work_type.encode()].qsize())
                job_data = None

                if self.task_queues[work_type.encode()].qsize() > 0:
                    job_data = self.task_queues[work_type.encode()].get()
                print("JOB DATA: {}".format(job_data))

                print(job_data)
                if job_data is not None:
                    print("INSIDE JOB_DATA")
                    job_data.item[0] = work_type.encode()
                    self.worker_socket.send_multipart(job_data.item)
                    self.worker_capacities[work_type] -= 1

    def kill_worker(self, identity):
        # TODO: Append kill message to queue.
        # TODO: Wrap up this stuff.
        print("KILL")


context = ZMQContext()

print("Creating client...")
client = Client(context)

worker_pool = WorkerPool(context)

results = queue.Queue()

worker_pool.create_worker('B')

# vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv #
#        MAIN LOOP BELOW           #
# vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv #

PARALLELISM = 4

while True:
    print("Getting new result")

    # Poll and get worker_id and result
    result = context.poller.poll()

    worker_msg = None
    client_msg = None

    print("Pulling messages from worker...")
    try:
        worker_msg = worker_pool.worker_socket.recv_multipart(flags=zmq.NOBLOCK)
        print(worker_msg)
        worker_result = pickle.loads(worker_msg[1])
        worker_command = pickle.loads(worker_msg[2])
    except zmq.ZMQError:
        print("No worker messages")
        pass

    # TODO 1: CREATE A function in WorkerPool that has simple scheduling that checks to see if we should change our worker_pool.
    # TODO 2: KILL ALL workers (i.e., append kill message to queue) OF WORKERS OF UNNEEDED TYPES.
    # TODO 3: CREATE/LAUNCH WORKERS OF THE NEEDED TYPE.
    # ##############

    # ##############

    print("Pulling messages from client...")

    work_type = b"*******"  # TODO: This is pointless...

    # Check to see if client message exists and add to appropriate queues.
    worker_pool.recv_client_message(client)

    # If we have a message from worker, process it.
    if worker_msg is not None:

        # TODO: Read the first-n bytes instead.

        # ##### TODO: WORKER REGISTRATION WORKFLOW ##### #
        # 1. Launch worker of type WORKER_TYPE. Create WORKER object with capacity = 0, put into task_type:workers dict.
        # 2. Listen for worker registration message. Switch capacity of worker_id to 1 (i.e., be listening for message.
        # 3. Add step that takes work from task_type queue and puts it onto individual worker queue.

        task_id = pickle.loads(worker_msg[1])
        worker_result = pickle.loads(worker_msg[2])
        worker_command = pickle.loads(worker_msg[3])
        task_type = worker_msg[4]

        # On registration, create worker and add to worker dicts.
        if worker_command == "REGISTER":
            # >>>>>>>>>>>>>>>> CONSTRUCTION ZONE >>>>>>>>>>>>>>>>
            worker_pool.register_worker(reg_message=worker_result)

        elif worker_command == "TASK_RETURN":
            print("In TASK RETURN: {}".format(worker_result))
            worker_pool.populate_results(worker_result)

            assert(results.qsize() > 0, "EmptyQueueError")

            # print("LATEST RESULT: {}".format(results.get()))  #
            worker_pool.send_results_to_client(client, results)

        else:
            raise NameError("[funcX] Unknown command type.")

    print(worker_pool.worker_capacities)

    # TODO: SWITCH entire model to hand task to individual works instead
    #           (FIFO workers' work_request queue -- rather than capacity?)

    print("Updating worker capacities...")
    worker_pool.update_worker_capacities()

    print("Sending results back to client...")
    worker_pool.send_results_to_client(client, results)

    time.sleep(0.5)

    # TODO: Have a set of killed workers; check the race condition where if you send a kill message to a worker after it has
    #   requested a task, the queue could look like TASK-KILL-TASK. Check against the set of killed workers;
    #   if the UUID is in there, then disregard.


