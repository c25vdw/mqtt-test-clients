from time import sleep
import paho.mqtt.client as mqtt
import argparse
import json
import uuid
import time
import math
from typing import List
from multiprocessing import Process, Value

parser = argparse.ArgumentParser(description="MQTT distributed dumb clients runner.")
parser.add_argument('--host', '-H', dest='host', type=str, help="host address of the broker", default="127.0.0.1")
parser.add_argument('--port', '-p', dest='port', type=int, help="port of the broker", default=1883)
parser.add_argument('--num', '-n', dest='num', type=int, help="number of duplcated clients", default=1)
parser.add_argument('--payload-size', '-P', dest='payload_size', type=int, help="extra payload (same for all clients)", default=10)
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
    n = args.num
    if args.num > 340:
        n = args.num / (math.floor(args.num / 340.0) + 1)
    
    for _ in range(int(n)):
        client = mqtt.Client(client_id=str(uuid.uuid4()))
        clients.append(client)
        client.connect(host=args.host, port=args.port)
        client.loop_start()
        client.on_message = on_message
        client.on_connect = on_connect
    

def start_poll_loop(args, avg_latency, avg_count):
    atomic_id = 0
    total_time = 30
    turns = int(total_time / args.interval)
    total_usage = 0
    for _ in range(turns):
        starting_times_atomic.append(time.time())
        average_time_diff[atomic_id] = (0, 0)
        sleep(args.interval)
        for c in clients:
            payload = json.dumps({
                'cid': str(c._client_id),
                'extra': args.payload_size * 'a',
                'aid': atomic_id
            })
            total_usage += len(payload)
            c.publish(f"req/{c._client_id}", payload)
        atomic_id += 1

    average = 0
    a = 0
    for (i, (key, val)) in enumerate(average_time_diff.items()):
        (avg, atomic) = val
        if i == 0:
            continue
        average = (average * a + avg) / (a + 1)
        a += 1
    print(f"average latency: {average}")
    print(f"total usage: {total_usage}, throughput {total_usage / total_time}")

    with avg_latency.get_lock():
        with avg_count.get_lock():
            avg_latency.value = (avg_latency.value * avg_count.value + average) / (avg_count.value + 1)
            avg_count.value += 1
    
def main(args, avg_latency, avg_count):
    init_clients(args)
    start_poll_loop(args, avg_latency, avg_count)

processes = []
print(f"creating {math.ceil(args.num / 340.0)} processes")
avg_latency = Value('d', 0)
avg_count = Value('d', 0)
for _ in range(math.ceil(args.num / 340.0)):
    
    p = Process(target = main, args=(args, avg_latency, avg_count))
    p.start()
    processes.append(p)

for p in processes:
    p.join()
print(f"final average: {avg_latency.value}")
