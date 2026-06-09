import socket
import logging

def verify_kafka():
    # 'host.docker.internal' allows Docker-hosted Airflow to communicate out to local machine's Kafka broker port
    host = "host.docker.internal" 
    port = 9092

    logging.info(f"Probing connection to Kafka broker at {host}:{port}...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5) # Prevent checking connection from hanging forever

    result = sock.connect_ex((host, port))
    sock.close()

    if result == 0:
        logging.info("Validation Success: Kafka Broker is ONLINE and accepting connections.")
    else:
        raise Exception(
            f"Validation Failure: Kafka Broker is OFFLINE at {host}:{port}. "
            "Please check if your docker-compose cluster or local runner is active."
        )