
import os
import zmq
import pickle
import logging
import argparse
from ipyparallel.serialize import serialize_object, unpack_apply_message, pack_apply_message

from parsl.app.errors import RemoteExceptionWrapper

# Keep this here for test function.
import time


class Worker:
    def __init__(self, identity, wid, port_addr):
        self.wid = wid
        self.con_id = None
        self.gpu_avail = None
        self.mem_avail = None
        self.data_obj = None

        self.service = "foxtrot"

        self.broker_path = "tcp://127.0.0.1:{}".format(port_addr)
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.identity = identity.encode()

        logger.debug("Creating worker of type: ".format(identity))
        logger.debug("Worker has identity: {}".format(self.wid.encode()))

        self.task_socket = self.context.socket(zmq.DEALER)
        self.task_socket.setsockopt(zmq.IDENTITY, self.wid.encode())
        logger.debug("Connecting to broker socket!")
        self.task_socket.connect(self.broker_path)
        self.poller.register(self.task_socket, zmq.POLLIN)

        logger.debug("Worker of type {} connected!".format(self.identity))


def execute_task(bufs):
    """Deserialize the buffer and execute the task.
    Returns the result or throws exception.
    """

    logger.debug("Inside execute_task function")
    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})

    logger.debug(bufs)

    f, actual_args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)

    logger.debug("Message unpacked")

    # We might need to look into callability of the function from itself
    # since we change it's name in the new namespace
    prefix = "parsl_"
    fname = prefix + "f"
    argname = prefix + "args"
    kwargname = prefix + "kwargs"
    resultname = prefix + "result"

    user_ns.update({fname: f,
                    argname: actual_args,
                    kwargname: kwargs,
                    resultname: resultname})

    logger.debug("Namespace updated")

    code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                           argname, kwargname)

    try:
        exec(code, user_ns, user_ns)

    except Exception as e:
        raise e

    else:
        return user_ns.get(resultname)


def listen_and_process(result, task_type, worker_type):
    logger.debug("Registering worker with broker...")
    while True:

        if type(worker_type) is not bytes:
            worker_type = worker_type.encode()

        logger.debug("Sending result...")
        worker.task_socket.send_multipart([pickle.dumps(""), pickle.dumps(result), pickle.dumps(task_type), worker_type, pickle.dumps(worker.wid)])
        bufs = None
        task_id = None

        logger.debug("Receiving message...")
        msg = worker.task_socket.recv_multipart()
        logger.info("MESSAGE: {}".format(msg))
        task_id = msg[0]

        if len(msg) == 1 and msg[0] == b"KILL":
            logger.info("KILLING -- Kill message received! ")
            exit()

        bufs = pickle.loads(msg[1])

        # TODO: Return this.
        worker_type = msg[3]
        #
        logger.debug("WORKER TYPE {}".format(worker_type))

        logger.debug("Executing task...")
        exec_result = execute_task(bufs)

        logger.debug("Executed result: {}".format(exec_result))

        # TODO: Change this to serialize_object to match IX?
        result = [pickle.dumps(task_id), exec_result.encode(), pickle.dumps("TASK_RETURN"), worker_type, worker.wid]
        time.sleep(2)
        logger.debug(result)
        task_type = "TASK_RETURN"


def start_file_logger(filename, rank, name='parsl', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.
    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string
    Returns:
       -  None
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d Rank:{0} [%(levelname)s]  %(message)s".format(rank)

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('-w', '--worker_type', default='A',
                        help="Worker type definition as string (Default=A)")

    parser.add_argument('-p', '--port_socket', default=50010,
                        help="Port on which to connect to broker ROUTER socket. (Default=50010)")

    parser.add_argument('-i', '--id_worker', default='no-id-supplied',
                        help="Port on which to connect to broker ROUTER socket. (Default=50010)")

    parser.add_argument("--logdir", help="Directory path where worker log files written", default='.')

    args = parser.parse_args()

    debug = True

    try:
        os.makedirs("{}/{}".format(args.logdir, 0))  # TODO: 0 should be a pool_id
    except FileExistsError:
        print("Logging directory already exists! Skipping...")

    start_file_logger('{}/{}/funcx_worker_{}.log'.format(args.logdir, 0, args.id_worker),
                      args.id_worker,
                      name="worker_log",
                      level=logging.DEBUG if debug else logging.INFO)

    worker = Worker(args.worker_type, args.id_worker, args.port_socket)

    result = {"wid": args.id_worker,
              "result": "REGISTER",
              "w_type": args.worker_type}
    task_type = "REGISTER"

    listen_and_process(result, task_type, args.worker_type)
