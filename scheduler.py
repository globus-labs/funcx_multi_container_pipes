

def naive_scheduler(task_queues, worker_capacities, max_workers):
    """ Return two items (as one tuple) dict kill_list :: KILL [(worker_type, num_kill), ...]
                                        dict create_list :: CREATE [(worker_type, num_create), ...]

        In this scheduler model, there is minimum 1 instance of each nonempty task queue.

    """

    # TODO: Just imagine two container types.
    # TODO: Always ensure there is at least one container for oldest submitted task.

    # kill_list = []
    # alive_list = []
    #
    # q_ranker = {}
    # total_tasks = 0
    #
    # for worker_type in task_queues:
    #     q_len = task_queues[worker_type].qsize()
    #     q_ranker[worker_type]
    #
    #

    kill_list = 5
    alive_list = 5

    return (kill_list, alive_list)