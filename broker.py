
import zmq
import uuid
import time
import queue
import pickle
import random
import scheduler
import subprocess

from typing import Any
from queue import PriorityQueue
from dataclasses import dataclass, field

# TODO 3: Ensure we can still mount to containers ('singularity run...') with fixes.
# TODO 7: Cleanups (including logging!) .
# TODO 8: Docs.
# TODO 9: FIFO Queue for workers requesting jobs.


@dataclass(order=True)
class PrioritizedItem:
    """ Helper class to create an item that can be read by a queue.PriorityQueue().
        :param priority (integer) -- higher is popped first from queue.
        :param item (object) -- any Python data structure we want in the queue.
        """
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
        self.client_socket.bind("tcp://*:50052")  # TODO: This should also use the Interchange port binding.
        self.client_identity = b"client_identity"

        # print("Client socket successfully created!")

    def results_to_client(self, res):
        # print("Sending results back to client...")
        self.client_socket.send_multipart([self.client_identity, pickle.dumps(res)])
        # print("Successfully sent results!")


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
        # TODO: Actually bring the container bits back to life on merge (post-fixing
        print("Launching new worker of container_mode: {}".format(self.container_mode))

        if self.container_mode is None:
            cmd = "python3 worker.py -w {} -p {} -i {}".format(self.worker_type, self.port_addr, self.wid)
        elif self.container_mode == "singularity_reuse":
            cmd = "singularity run --writable {} REUSE".format(self.container_uri)
        elif self.container_mode == "no_reuse":
            cmd = "singularity run --writable {} NO_REUSE".format(self.container_uri)
        else:
            raise NameError("Invalid container launch mode.")

        exit_code = subprocess.Popen(cmd, shell=True)
        return exit_code


class WorkerPool(object):

    def __init__(self, z_context):

        # TODO: Write more docs as to what is in these things.
        self.task_queues = {}  # k-v: task_type - task_q (PriorityQueue)

        self.worker_capacities = {}  # k-v: worker_id - capacity (integer... should only ever be 0 or 1).
        # TODO: Switch ^^^ to FIFO task queue.

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
        # TODO: Look at Interchange version of code to add addresses. Line 198 of htex.
        # if worker_port specified, try to connect... if error, just report it.
        # if none specified, then
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

            # TODO: If does not already exist, add a new task_queue.
            if work_type.decode() not in self.task_queues:
                self.task_queues[work_type.decode()] = PriorityQueue()
                self.task_to_worker_sets[work_type.decode()] = set()

            pri_queue = self.task_queues[work_type.decode()]
            pri_queue.put(PrioritizedItem(5, client_msg))

        except zmq.ZMQError:
            # print("No client messages")
            pass

    def send_results_to_client(self, cli, res_q):

        # TODO: Move this static function to the client instead.
        # print("Checking length of results...")
        while not results.empty():
            result = res_q.get()
            result.insert(0, cli.client_identity)
            cli.results_to_client(result)

    def populate_results(self, worker_result):
        # TODO: be sending byte strings.
        worker_task_type = pickle.loads(worker_msg[5]) # Parse out the worker_type.
        print("WORK TYPE: {}".format(worker_task_type))

        self.worker_capacities[worker_task_type] += 1  # Add 1 back to the capacity.

        # Receive from the worker.
        try:
            # print("RESULT RECEIVED")
            results.put(worker_result)
            # print("RESULTS: {}".format(results))
        except zmq.ZMQError:
            # print("Nothing to task_return.")
            pass

    def register_worker(self, reg_message):
        # Set our initial capacity to 1, meaning we're ready for a task.
        self.worker_capacities[reg_message["wid"]] += 1  # Worker now ready to receive a task!
        assert(self.worker_capacities == 1, "Capacity is zero. ")
        print("Successfully registered worker! ")

    def assign_to_workers(self):
        for task_type in self.task_queues:

            print("[ASSIGN] Queue size: {}".format(self.task_queues[task_type].qsize()))

            # If there are tasks in the queue and a positive number of workers.
            if self.task_queues[task_type].qsize() > 0 and len(self.task_to_worker_sets[task_type]) > 0:

                task = self.task_queues[task_type].get()

                for wid in self.task_to_worker_sets[task_type]:
                    job_data = None
                    assert (results.qsize() > 0, "EmptyQueueError")

                    # IF the worker has available capacity (and is not dead)
                    if self.worker_capacities[wid] > 0 or wid in self.dead_worker_set:  # and wid not in self.dead_worker_set:
                        print("{} HAS available capacity!".format(wid))
                        job_data = task
                    else:
                        print("{} DOES NOT HAVE available capacity".format(wid))

                    if job_data is not None:

                        if job_data.item[1] == b"KILL":
                            print("***** Sending KILL MESSAGE to {} *****".format(wid))
                            self.dead_worker_set.add(wid)

                        job_data.item[0] = wid.encode()

                        # print(job_data.item)

                        self.worker_socket.send_multipart(job_data.item)
                        self.worker_capacities[wid] -= 1
                        assert (self.worker_capacities[wid] >= 0, "Invalid capacity count")

    def kill_workers(self, identity_tuples):

        # Receive a list of tuples of worker_type, # to kill.
        if identity_tuples is not None:
            for id_tup in identity_tuples:
                w_type, num_kill = id_tup

                print("[KILL] Worker_Type: {}".format(w_type))
                print("[KILL] Num_to_Kill: {}".format(num_kill))

                killed_workers = 0

                # pri_queue = PriorityQueue()

                # Kill num_kill distinct worker ids
                print("Eligible killable workers: {}".format(self.task_to_worker_sets[w_type]))

                print(len(self.task_to_worker_sets[w_type].copy()))

                for wid in self.task_to_worker_sets[w_type].copy():

                    print("WORKER_ID FOR CONSIDERATION {}".format(wid))

                    # Pick worker that's waiting for work (if any).
                    if self.worker_capacities[wid] == 1 and killed_workers < num_kill:  # if waiting.
                        print("Killing bored workers!")
                        self.worker_capacities[wid] = 0  # Set to zero.
                        self.dead_worker_set.add(wid)  # add to dead_pool.

                        # Append KILL message to its queue.
                        pri_queue = self.task_queues[w_type]

                        # Append KILL message to its queue.
                        pri_queue.put(PrioritizedItem(5, [wid.encode(), b"KILL"]))
                        killed_workers += 1

                        # Now remove the worker_id from the task_to_worker set.
                        # self.task_to_worker_sets[w_type].remove(wid)

                    elif killed_workers < num_kill:
                        print("Killing busy worker!")
                        print(wid)
                        # print("Have to kill dead busy workers!")
                        self.dead_worker_set.add(wid)  # add to dead_pool.

                        # Append KILL message to its queue.
                        pri_queue = self.task_queues[w_type]
                        pri_queue.put(PrioritizedItem(5, [wid.encode(), b"KILL"]))
                        killed_workers += 1

                        # self.task_to_worker_sets[w_type].remove(wid)

                    else:
                        print("Oops, wound up in here. ")
                        break

                        # Now remove the worker_id from the task_to_worker set.
                        # self.task_to_worker_sets[w_type].remove(wid)
                    print(killed_workers)
                    print(pri_queue.qsize())


            # print(">>> FINISHED WORKER_KILL PHASE :))))")


if __name__ == "__main__":

    context = ZMQContext()

    # print("Creating client...")
    client = Client(context)

    worker_pool = WorkerPool(context)

    results = queue.Queue()

    # # TODO. Make this line unnecessary.
    # worker_pool.create_worker('B')
    # worker_pool.create_worker('A')
    # worker_pool.create_worker('B')
    # worker_pool.create_worker('A')
    #
    # print(worker_pool.task_to_worker_sets)
    # worker_pool.kill_workers([('A', 2)])
    # kill_list = [('A', 2)]



    # print(worker_pool.task_to_worker_sets)
    #

    # worker_pool.create_worker('A')

    # print("DEAD WORKER POOL: ")
    # print(worker_pool.dead_worker_set)

    # vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv #
    #        MAIN LOOP BELOW           #
    # vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv #

    PARALLELISM = 4
    alive_list = None
    kill_list = None

    while True:

        print("TASK:WORKER Sets: {}".format(worker_pool.task_to_worker_sets))

        # print("Getting new result")
        # Poll and get worker_id and result
        # TODO: Check to see if worker_msg or client_msg in poll (don't do the noblock here).
        result = context.poller.poll()

        worker_msg = None
        client_msg = None

        # print("Pulling messages from worker...")
        try:
            worker_msg = worker_pool.worker_socket.recv_multipart(flags=zmq.NOBLOCK)
            worker_result = pickle.loads(worker_msg[1])
            worker_command = pickle.loads(worker_msg[2])
        except zmq.ZMQError:
            # print("No worker messages")
            pass


        # print("Pulling messages from client...")

        # Check to see if client message exists and add to appropriate queues.
        worker_pool.recv_client_message(client)

        # *** TODO: HERE IS WHERE WE NEED TO SPIN DOWN AND THEN SPIN UP WORKERS. ***

        # TODO: Add back.
        # SCHEDULER: Get two lists of tuples containing (worker_type, # workers to create/kill).

        alive_list, kill_list = scheduler.naive_scheduler(worker_pool.task_queues,
                                                          worker_pool.task_to_worker_sets, max_workers=PARALLELISM)

        print("Create: {}".format(alive_list))
        print("Kill: {}".format(kill_list))

        # TODO: Why isn't the client driving correctly?
        # First we want to kill all of the unnecessary containers.
        if kill_list is not None:
            print("Processing KILL list...")
            worker_pool.kill_workers(kill_list)
            print("Dead workers: {}".format(worker_pool.dead_worker_set))

        # kill_list = []

        #
        # # # Next we want to bring all of the new containers to life.
        if alive_list is not None:
            print("Processing ALIVE list...")
            for alive_tup in alive_list:
                for i in range(1, alive_tup[1]+1):
                    # Worker type
                    worker = worker_pool.create_worker(alive_tup[0])

        # **************************************************************************

        # If we have a message from worker, process it.
        if worker_msg is not None:

            # TODO: Read the first-n bytes instead.
            task_id = pickle.loads(worker_msg[1])

            # TODO: Result and command repeated below.
            # TODO: Put command before result.
            worker_result = pickle.loads(worker_msg[2])
            worker_command = pickle.loads(worker_msg[3])
            task_type = worker_msg[4]

            # On registration, create worker and add to worker dicts.
            if worker_command == "REGISTER":
                worker_pool.register_worker(reg_message=worker_result)

            elif worker_command == "TASK_RETURN":
                # print("In TASK RETURN: {}".format(worker_result))
                worker_pool.populate_results(worker_result)

                assert(results.qsize() > 0, "EmptyQueueError")

                worker_pool.send_results_to_client(client, results)

            else:
                raise NameError("[funcX] Unknown command type.")

        # TODO: (FIFO workers' work_request queue -- rather than capacity?)

        # print("Updating worker capacities...")
        worker_pool.assign_to_workers()

        # print("Sending results back to client...")
        worker_pool.send_results_to_client(client, results)

        time.sleep(0.5)
