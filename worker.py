
import zmq
import uuid
import pickle
import argparse
import logging
from ipyparallel.serialize import serialize_object, unpack_apply_message, pack_apply_message


# Keep this here for test function.
import time

# logging.basicConfig(filename='example.log', level=logging.DEBUG)


class Worker:
    def __init__(self, identity):
        self.wid = uuid.uuid4()
        self.con_id = None
        self.gpu_avail = None
        self.mem_avail = None
        self.data_obj = None

        self.service = "foxtrot"

        # self.broker_path = "tcp://*:50001"
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.identity = identity.encode()

        print("Identity: ".format(identity))

        self.task_socket = self.context.socket(zmq.DEALER)
        self.task_socket.setsockopt(zmq.IDENTITY, b"A")  # TODO
        print("Connecting to broker socket!")
        self.task_socket.connect("tcp://127.0.0.1:50010")
        self.poller.register(self.task_socket, zmq.POLLIN)

        print("Worker of type {} connected!".format(self.identity))


def execute_task(bufs):
    """Deserialize the buffer and execute the task.
    Returns the result or throws exception.
    """

    print("Inside execute_task function")
    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})

    print(bufs)

    f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)

    print("Message unpacked")

    # We might need to look into callability of the function from itself
    # since we change it's name in the new namespace
    prefix = "parsl_"
    fname = prefix + "f"
    argname = prefix + "args"
    kwargname = prefix + "kwargs"
    resultname = prefix + "result"

    user_ns.update({fname: f,
                    argname: args,
                    kwargname: kwargs,
                    resultname: resultname})

    print("Namespace updated")

    code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                           argname, kwargname)

    try:
        exec(code, user_ns, user_ns)

    except Exception as e:
        raise e

    else:
        return user_ns.get(resultname)


def listen_and_process(result, task_type):
    print("Registering worker with broker...")
    while True:
        # TODO: Make this line async.
        print("Sending result...")
        # result = "Potato"
        # task_type = "tomato"
        worker.task_socket.send_multipart([pickle.dumps(""), pickle.dumps(result), pickle.dumps(task_type)])
        bufs = None

        print("Receiving message...")
        msg = worker.task_socket.recv_multipart()
        #print(msg)
        task_id = msg[0]
        bufs = pickle.loads(msg[1])
        #print(bufs)

        print("Executing task...")
        exec_result = execute_task(bufs)

        print("Executed result: {}".format(exec_result))

        task_id = "THISISTASKID"

        # TODO: Should this be pack_apply_object or serialize_object?
        result = [pickle.dumps(task_id), exec_result.encode(), pickle.dumps("TASK_RETURN")]
        time.sleep(2)
        print(result)
        task_type = "TASK_RETURN"


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('-w', '--worker_type', default='A',
                        help="Worker type definition as string (Default=A)")

    # parser.add_argument('-s', '--socket_url', default=50000,
    #                     help="Worker type definition as string (Default=A)")

    args = parser.parse_args()

    worker = Worker(args.worker_type)

    result = {"wid": 'potato',
              "result": "REGISTER",
              "w_type": "A"}
    task_type = "REGISTER"

    listen_and_process(result, task_type)
