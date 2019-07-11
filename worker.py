
import zmq
import uuid
import pickle

# Keep this here for test function.
import time


class Worker:
    def __init__(self, identity):
        self.wid = uuid.uuid4()
        self.con_id = None
        self.gpu_avail = None
        self.mem_avail = None
        self.data_obj = None

        self.service = "echo"

        self.broker_path = "tcp://localhost:50001"
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.identity = identity

        self.task_socket = self.context.socket(zmq.DEALER)
        self.task_socket.setsockopt(zmq.IDENTITY, b'A')
        print("Connecting to broker socket!")
        self.task_socket.connect(self.broker_path)
        self.poller.register(self.task_socket, zmq.POLLIN)


worker = Worker("A")

# TODO: Move much of this back to the manager-level.
result = {"wid": worker.wid,
          "con_id": worker.con_id,
          "gpu_avail": worker.gpu_avail,
          "mem_avail": worker.mem_avail,
          "data_obj": worker.data_obj,
          "result": "REGISTER",
          "w_type": "A"}
task_type = "REGISTER"


print("Registering worker with broker...")


while True:
    # TODO: Make this line async.
    print("Sending result...")
    worker.task_socket.send_multipart([pickle.dumps(""), pickle.dumps(result), pickle.dumps(task_type)])

    print("Receiving message...")
    msg = worker.task_socket.recv_multipart()[0].decode()
    print(msg)

    print("Executing task...")
    result = [pickle.dumps(""), pickle.dumps(exec(msg)), pickle.dumps("TASK_RETURN")]
    time.sleep(2)
    print(result)
    task_type = "TASK_RETURN"
