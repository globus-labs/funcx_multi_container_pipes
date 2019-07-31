
import math


def naive_scheduler(task_queues, task_to_worker_sets, max_workers):
    """ Return two items (as one tuple) dict kill_list :: KILL [(worker_type, num_kill), ...]
                                        dict create_list :: CREATE [(worker_type, num_create), ...]

        In this scheduler model, there is minimum 1 instance of each nonempty task queue.

    """

    kill_list = []
    create_list = []

    kill_set = set()
    alive_set = set()

    total_kill = 0
    total_tasks = 0

    for worker_type in task_queues:
        q_len = task_queues[worker_type].qsize()

        print("Worker Type: {}, Q_size: {}".format(worker_type, q_len))

        # Really just using this loop to sum the total size of all task queues.
        total_tasks += q_len

        # Always leave one container in case of empty queue.
        if q_len == 0:
            kill_count = len(task_to_worker_sets[worker_type])-1
            kill_list.append((worker_type, kill_count))
            kill_set.add(worker_type)

            total_kill += kill_count

            # If the queue is already empty and no workers exist for it, then create one!
            if len(task_to_worker_sets[worker_type]) == 0:
                print("LEN is ZERO")
                create_list.append((worker_type, 1))
                alive_set.add(worker_type)

    if total_tasks == 0:
        return [], []

    # Now loop again to account for more-complex cases.
    for worker_type in task_queues:

        if worker_type not in kill_set and worker_type not in alive_set:
            print("IN HERE")
            q_len = task_queues[worker_type].qsize()
            num_containers = max(1, math.floor(max_workers*(q_len/total_tasks)))

            if len(task_to_worker_sets[worker_type]) < num_containers:
                print("here1")
                create_list.append((worker_type, num_containers - len(task_to_worker_sets[worker_type])))
            elif len(task_to_worker_sets[worker_type]) > num_containers:
                print("here2")
                kill_list.append((worker_type, len(task_to_worker_sets[worker_type]) - num_containers))

    return create_list, kill_list


# from queue import Queue
#
# q_1 = Queue()
# q_1.put(1)
# q_1.put(2)
# q_1.put(3)
# q_1.put(4)
# q_1.put(5)
#
# q_2 = Queue()
# q_2.put(1)
# # q_2.put(2)
# # q_2.put(3)
# # q_2.put(4)
# # q_2.put(5)
#
# task_queues = {1: q_1, 2: q_2}
#
# task_worker_set = {1: {1,2,3}, 2: {1,2,3}}
#
# max_workers = 4
#
# num_workers = {1: 3, 2: 0}
#
# print(naive_scheduler(task_queues, task_worker_set, max_workers, num_workers))
