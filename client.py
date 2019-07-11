
import zmq
import pickle
import uuid

context = zmq.Context()
broker_socket = context.socket(zmq.DEALER)
broker_socket.setsockopt(zmq.IDENTITY, "B".encode())

broker_socket.connect("tcp://localhost:50002")


tasks = ["print('hello')", "print('hello2')",  "print('hello3')",  "print('hello4')",  "print('hello5')"]

for req in range(1):
    print("Sending request {}".format(req))
    broker_socket.send_multipart([pickle.dumps(uuid.uuid4), pickle.dumps(tasks), pickle.dumps("INVOKE")])

    # TODO: NOBLOCK receive results.
    # message = None

    #try:
    message = broker_socket.recv_multipart()
    #print(pickle.loads(message[1]))
    # except zmq.ZMQError:
    #     print("No message received by client!")
    #     pass
    #
    # if message is not None:
    #     print(message[1], message[2])


