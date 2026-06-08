import socket

def verify_kafka():

    host = "localhost"
    port = 9092

    sock = socket.socket()

    result = sock.connect_ex(
        (host, port)
    )

    if result == 0:
        print("Kafka Running")
    else:
        raise Exception(
            "Kafka Offline"
        )