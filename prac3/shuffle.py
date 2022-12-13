import argparse
import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))
channel = connection.channel()


def send(pairs):
    exchange = "shuffle-reducer"
    let_num = ord('z') - ord('a')
    for pair in pairs.items():
        reducer_id = int(
            (float(ord(pair[0][0]) - ord('a')) / (let_num + 1)) * args.k)
        routing_key = "shuffle_"+str(reducer_id)
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.queue_declare(routing_key)
        channel.queue_bind(queue=routing_key, exchange=exchange)
        channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=json.dumps(pair)
        )


parser = argparse.ArgumentParser()
parser.add_argument(
    "-i", "--id", help="Shuffle ID", default='0'
)
parser.add_argument(
    "-k", help="Number of reducers", default=1, type=int
)

args = parser.parse_args()

channel.queue_declare("mapper_"+args.id)

pair = ('', 0)
method_frame = 0
while not method_frame:
    method_frame, header_frame, body = channel.basic_get(queue="mapper_"+args.id,
                                                         auto_ack=False)
    if method_frame:
        channel.basic_ack(method_frame.delivery_tag)
        pair = json.loads(body.decode("utf-8"))
pairs = {}
while pair[0] != '!The end!':

    method_frame, header_frame, body = channel.basic_get(queue="mapper_"+args.id,
                                                         auto_ack=False)
    if method_frame:
        channel.basic_ack(method_frame.delivery_tag)
        pair = json.loads(body.decode("utf-8"))
        if pair[0] != '!The end!':
            if pair[0] in pairs.keys():
                pairs[pair[0]].append(pair[1])
            else:
                pairs[pair[0]] = [pair[1]]


send(pairs)
exchange = "shuffle-reducer"
for i in range(args.k):
    routing_key = "shuffle_"+str(i)
    channel.exchange_declare(exchange=exchange, exchange_type="direct")
    channel.basic_publish(
        exchange=exchange, routing_key=routing_key, body=json.dumps(
            ("!The end!", int(args.id)))
    )
