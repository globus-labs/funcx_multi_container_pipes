
import zmq
import uuid
import time
import queue
import pickle
import random
import subprocess


from queue import PriorityQueue

# TODO: TODAY/TOMORROW.

# TODO 0: think of all workers as waiting for either WORK or KILL.
# TODO 1: Spin up containers automatically using subprocess (like process worker pool).
# TODO 2: Use actual containers with the workers (if container_reuse or container_single_use).
# TODO 3: Receiving task and killing worker in 'no_reuse' should happen back-to-back.
# TODO 4: Implement a strategy.
# TODO 5: Figure out how to pump in work really fast.
# TODO 6: Track resources on the system (can then use this for scheduling purposes).


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


class Worker(object):
    def __init__(self, worker_type, port_addr, container_uri=None, container_mode=None):
        self.capacity = 0
        self.container_mode = container_mode
        self.container_uri = container_uri
        self.port_addr = port_addr
        self.task_q = queue.Queue()  # This will be the local queue of each worker.
        self.wid = str(uuid.uuid4())
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



class WorkerPool(object):

    def __init__(self, z_context):

        self.task_queues = {}  # k-v: task_type - task_q (PriorityQueue)

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

        # Keep trying until we get a non-conflicting socket_address.
        while True:
            self.sock_addr = random.choice(self.socket_range)
            if self.sock_addr not in self.sockets_in_use:
                self.sockets_in_use.add(self.sock_addr)
                break
            else:
                print("Conflicting socket... Retry...")

        self.worker_socket.bind("tcp://*:{}".format(self.sock_addr))
        self.dead_worker_set = set()

    def create_worker(self, worker_type, container=None):

        worker = Worker(worker_type, self.sock_addr, container_uri=None, container_mode=None)  # TODO: container_* shouldn't be hardcoded.
        wid = worker.wid

        # Add to the queues
        if worker_type not in self.task_queues:
            self.task_queues[worker_type] = PriorityQueue()  # Create queue if not one for this worker type.
        if worker_type not in self.worker_capacities:  # Init wid's capacity to be zero until we finish setting it up.
            self.worker_capacities[wid] = 0
        if worker_type not in self.task_to_worker_sets:
            self.task_to_worker_sets[worker_type] = set()
            self.task_to_worker_sets[worker_type].add(wid)
        else:
            self.task_to_worker_sets[worker_type].add(wid)

        worker.launch()

        print("Successfully initialized worker! ")
        return "DONE"

    def recv_client_message(self, cli):
        try:
            client_msg = cli.client_socket.recv_multipart(flags=zmq.NOBLOCK)
            work_type = client_msg[-1]

            # print("DEBUG >> WORK TYPE: {}".format(type(work_type)))
            # print(work_type)

            pri_queue = self.task_queues[work_type.decode()]
            pri_queue.put(PrioritizedItem(5, client_msg))
            # print("Priority Queue size: {}".format(pri_queue.qsize()))
            # print("Work type: {}".format(work_type.decode()))

            # print(pri_queue.get())


        except zmq.ZMQError:
            print("No client messages")

    def send_results_to_client(self, cli, res_q):

        # TODO: Move this static function to the client instead.

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
        worker_task_type = pickle.loads(worker_msg[5]) # Parse out the worker_type.  # TODO.
        print("WORK TYPE: {}".format(worker_task_type))

        self.worker_capacities[worker_task_type] += 1  # Add 1 back to the capacity.

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
        self.worker_capacities[reg_message["wid"]] += 1  # Worker now ready to receive a task!
        assert(self.worker_capacities == 1, "Capacity is zero. ")
        print("Successfully registered worker! ")

    def assign_to_workers(self):
        for task_type in self.task_queues:
            print(task_type)

            # If there are tasks in the queue and a positive number of workers.
            if self.task_queues[task_type].qsize() > 0 and len(self.task_to_worker_sets[task_type]) > 0:
                task = self.task_queues[task_type].get()

                for wid in self.task_to_worker_sets[task_type]:
                    job_data = None
                    assert (results.qsize() > 0, "EmptyQueueError")

                    # IF the worker has available capacity (and is not dead)
                    if self.worker_capacities[wid] > 0 and wid not in self.dead_worker_set:
                        job_data = task

                    if job_data is not None:
                        job_data.item[0] = wid.encode()
                        self.worker_socket.send_multipart(job_data.item)
                        self.worker_capacities[wid] -= 1
                        assert (self.worker_capacities[wid] >= 0, "Invalid capacity count")

    def kill_workers(self, identity_tuples):

        # TODO: Clean up and test if working (esp. the sending empty byte list?).

        # Receive a list of tuples of worker_type, # to kill.
        if identity_tuples is not None:
            for id_tup in identity_tuples:
                id, num_kill = id_tup

                killed_workers = 0
                # Get num_kill distinct worker ids
                for _ in range(1, num_kill+1):

                    # Pick worker that's waiting for work (if any).
                    for wid in self.task_to_worker_sets[id]:
                        if self.worker_capacities[wid] == 1:  # if waiting.
                            self.worker_capacities[wid] = 0  # Set to zero.
                            self.dead_worker_set.add(wid)  # add to dead_pool.

                            # Append KILL message to its queue.
                            pri_queue = self.task_queues[work_type]
                            pri_queue.put(PrioritizedItem(5, [b""]))  # TODO: See if kill works.

                            killed_workers += 1

                        # Now if need to kill busy workers...
                        if killed_workers < num_kill:
                            self.dead_worker_set.add(wid)  # add to dead_pool.

                            # Append KILL message to its queue.
                            pri_queue = self.task_queues[work_type]
                            pri_queue.put(PrioritizedItem(5, b""))

                            killed_workers += 1


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

        task_id = pickle.loads(worker_msg[1])
        worker_result = pickle.loads(worker_msg[2])
        worker_command = pickle.loads(worker_msg[3])
        task_type = worker_msg[4]

        # On registration, create worker and add to worker dicts.
        if worker_command == "REGISTER":
            worker_pool.register_worker(reg_message=worker_result)

        elif worker_command == "TASK_RETURN":
            print("In TASK RETURN: {}".format(worker_result))
            worker_pool.populate_results(worker_result)

            assert(results.qsize() > 0, "EmptyQueueError")

            # print("LATEST RESULT: {}".format(results.get()))  #
            worker_pool.send_results_to_client(client, results)

        else:
            raise NameError("[funcX] Unknown command type.")

    # TODO: SWITCH entire model to hand task to individual works instead
    #           (FIFO workers' work_request queue -- rather than capacity?)

    print("Updating worker capacities...")
    worker_pool.assign_to_workers()

    print("Sending results back to client...")
    worker_pool.send_results_to_client(client, results)

    time.sleep(0.5)
