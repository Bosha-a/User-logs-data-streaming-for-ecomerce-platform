import socket
import random
import json
import time
from datetime import datetime

# OPTIONAL - Uncomment if you want to actually push to Kafka
# from kafka import KafkaProducer
# producer = KafkaProducer(
#     bootstrap_servers="localhost:9092",
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

def generate_random_ip():
    """Generate a realistic IPv4 address."""
    return ".".join(str(random.randint(1, 254)) for _ in range(4))

def resolve_ip(ip):
    """
    Returns hostname, alias list, address list.
    Handles DNS errors cleanly.
    """
    try:
        hostname, alias_list, address_list = socket.gethostbyaddr(ip)
        return hostname, alias_list, address_list, "success"
    except Exception:
        return None, [], [], "unknown_host"

def build_event():
    """Build one full, comprehensive JSON event."""
    ip = generate_random_ip()
    hostname, alias_list, address_list, status = resolve_ip(ip)

    event = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "client_ip": ip,
        "hostname": hostname,
        "alias_list": alias_list,
        "address_list": address_list,
        "lookup_status": status,
        "response_time_ms": random.randint(1, 50),
        "source": "python-simulator-01"
    }

    return event


def stream_data(interval=1, send_to_kafka=False):
    """Continuously generate and print/send events."""
    while True:
        event = build_event()
        print(json.dumps(event, indent=2))

        # If Kafka enabled, send
        # if send_to_kafka:
        #     producer.send("dns_logs", event)

        time.sleep(interval)


if __name__ == "__main__":
    stream_data(interval=1, send_to_kafka=False)