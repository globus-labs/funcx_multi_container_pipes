
import zmq
import pickle

# TODO: Connect to broker using REQ/REP.

context = zmq.Context()
broker_socket = context.socket(zmq.DEALER)


broker_socket.connect("tcp://localhost:50002")

for req in range(10):
    print("Sending request {}".format(req))
    broker_socket.send_multipart([pickle.dumps("############################################"), pickle.dumps("INVOKE")])

    # message = broker_socket.recv()

    # print("Received reply %s [ %s ]" % (req, message))






