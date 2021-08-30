import paho.mqtt.client as mqtt
import uuid
import json
import argparse

parser = argparse.ArgumentParser(description="MQTT distributed dumb clients runner.")
parser.add_argument('--host', '-H', dest='host', type=str, help="host address of the broker", default="127.0.0.1")
parser.add_argument('--port', '-p', dest='port', type=int, help="port of the broker", default=1883)
parser.add_argument('--num', '-n', dest='num', type=int, help="number of duplcated clients", default=1)
parser.add_argument('--extra-payload', '-P', dest='extra_payload', type=str, help="extra payload (same for all clients)")
parser.add_argument('--interval', '-i', dest='interval', type=int, help="sleep between two publish (same for all clients)", default=1)

args = parser.parse_args()

print(args.host, args.port, args.num, args.extra_payload, args.interval)
def on_message(c, userdata, msg):

    payload = json.loads(msg.payload)
    cid = payload['cid']
    aid = payload['aid']
    extra = payload['extra']

    c.publish(f"resp/{cid}", json.dumps(payload))


def on_connect(c, userdata, flags, rc):
    c.subscribe(f"req/#")

client = mqtt.Client(client_id=str(uuid.uuid4()))
client.on_message = on_message
client.on_connect = on_connect
client.connect(args.host, args.port)
client.loop_forever()
