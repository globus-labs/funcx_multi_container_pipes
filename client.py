
import zmq

# TODO: Connect to broker using REQ/REP.

context = zmq.Context()
broker_socket = context.socket(zmq.REQ)


broker_socket.connect("tcp://localhost:50005")

for req in range(10):
    print("Sending request {}".format(req))
    broker_socket.send(b"Hello")

    message = broker_socket.recv()

    print("Received reply %s [ %s ]" % (req, message))






