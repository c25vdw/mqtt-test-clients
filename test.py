from time import sleep
import paho.mqtt.client as mqtt
import argparse
import json
import uuid
import time
from typing import List

parser = argparse.ArgumentParser(description="MQTT distributed dumb clients runner.")
parser.add_argument('--host', '-H', dest='host', type=str, help="host address of the broker", default="127.0.0.1")
parser.add_argument('--port', '-p', dest='port', type=int, help="port of the broker", default=1883)
parser.add_argument('--num', '-n', dest='num', type=int, help="number of duplcated clients", default=1)
parser.add_argument('--payload-size', '-P', dest='payload_size', type=int, help="extra payload (same for all clients)")
parser.add_argument('--interval', '-i', dest='interval', type=int, help="sleep between two publish (same for all clients)", default=1)

args = parser.parse_args()

print(args.host, args.port, args.num, args.payload_size, args.interval)

clients: List[mqtt.Client] = []
starting_times_atomic: List[float] = []
average_time_diff = {}

def on_message(client, userdata, msg):
    payload = json.loads(msg.payload)
    aid = payload['aid']

    diff = time.time() - starting_times_atomic[aid] - args.interval
    avg, count = average_time_diff[aid]

    average_time_diff[aid] = ((avg*count + diff)/(count+1), count + 1)

def on_connect(c, userdata, flags, rc):
    c.subscribe(f"resp/{c._client_id}")

# initialize all clients
def init_clients(args):
    for _ in range(args.num):
        client = mqtt.Client(client_id=str(uuid.uuid4()))
        client.loop_start()
        clients.append(client)
        client.connect(host=args.host, port=args.port)
        client.on_message = on_message
        client.on_connect = on_connect
    # payload = {
    #     'resp': str(client._client_id),
    #     'extra': args.extra_payload
    # }


def start_poll_loop(args):
    atomic_id = 0
    while True:
        starting_times_atomic.append(time.time())
        average_time_diff[atomic_id] = (0, 0)
        sleep(args.interval)
        print(average_time_diff)
        for c in clients:
            payload = json.dumps({
                'cid': str(c._client_id),
                'extra': args.payload_size * 'a',
                'aid': atomic_id
            })
            c.publish(f"req/{c._client_id}", payload)
        atomic_id += 1


if __name__ == '__main__':
    init_clients(args)
    start_poll_loop(args)
