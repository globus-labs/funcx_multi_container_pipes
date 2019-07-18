#
import zmq
import time
import random

# class ZMQContext(object):
#     def __init__(self):
#         self.context = zmq.Context()
#         self.poller = zmq.Poller()
#
#
# class Client(object):
#     def __init__(self, z_context):
#         self.context = z_context.context
#         # self.poller = z_context.poller
#         self.client_socket = self.context.socket(zmq.ROUTER)
#         # self.poller.register(self.client_socket, zmq.POLLIN)
#         self.client_socket.bind("tcp://*:56473")
#
# class Worker:
#     def __init__(self, identity, wid, port_addr):
#         self.wid = wid
#         self.con_id = None
#         self.gpu_avail = None
#         self.mem_avail = None
#         self.data_obj = None
#
#         self.service = "foxtrot"
#
#         self.broker_path = "tcp://127.0.0.1:{}".format(port_addr)
#         print(self.broker_path)
#
#         self.context = zmq.Context()
#         self.poller = zmq.Poller()
#         self.identity = identity.encode()
#
#         print("Creating worker of type: ".format(identity))
#
#         self.task_socket = self.context.socket(zmq.ROUTER)
#         print(identity)
#         self.task_socket.setsockopt(zmq.IDENTITY, self.identity)
#         print("Connecting to broker socket!")
#         self.task_socket.connect(self.broker_path)  # TODO: Bring back random provisioning of port number.
#         self.poller.register(self.task_socket, zmq.POLLIN)
#
#         print("Worker of type {} connected!".format(self.identity))
#
#
#
#
#         # print("Client socket successfully created!")
#
#
# # context = ZMQContext()
# # client = Client(context)
#
# worker = Worker('B', 'poooooool', '59999')
#
# print("Sending message")
# worker.task_socket.send_multipart([b'B', b'HELLO'])
# print("SENT!")
#
# import time
# import random
# from threading import Thread
#
# import zmq
#
# # We have two workers, here we copy the code, normally these would
# # run on different boxes…
# #
# def worker_a(context=None):
#     context = context or zmq.Context.instance()
#     worker = context.socket(zmq.DEALER)
#     worker.setsockopt(zmq.IDENTITY, b'A')
#     worker.connect("tcp://127.0.0.1:55555")
#
#     total = 0
#     while True:
#         # We receive one part, with the workload
#         request = worker.recv()
#         finished = request == b"END"
#         if finished:
#             print("A received: %s" % total)
#             break
#         total += 1
#
# def worker_b(context=None):
#     context = context or zmq.Context.instance()
#     worker = context.socket(zmq.DEALER)
#     worker.setsockopt(zmq.IDENTITY, b'B')
#     worker.connect("tcp://127.0.0.1:55555")
#
#     total = 0
#     while True:
#         # We receive one part, with the workload
#         request = worker.recv()
#         finished = request == b"END"
#         if finished:
#             print("B received: %s" % total)
#             break
#         total += 1

context = zmq.Context.instance()
client = context.socket(zmq.ROUTER)
client.bind("tcp://*:55555")

# Thread(target=worker_a).start()
# Thread(target=worker_b).start()

# Wait for threads to stabilize
time.sleep(1)

# Send 10 tasks scattered to A twice as often as B
for _ in range(10):
    # Send two message parts, first the address…
    ident = random.choice([b'B', b'B', b'B'])
    # And then the workload
    work = b"This is the workload"
    client.send_multipart([ident, work])

client.send_multipart([b'B', b'END'])