import argparse
import pika
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))
channel = connection.channel()


def send(pairs):
    exchange = "reducer-master"
    routing_key = "last_step"
    channel.exchange_declare(exchange=exchange, exchange_type="direct")
    channel.queue_declare(routing_key)
    channel.queue_bind(queue=routing_key, exchange=exchange)
    channel.basic_publish(
        exchange=exchange, routing_key=routing_key, body=json.dumps(pairs)
    )


parser = argparse.ArgumentParser()
parser.add_argument(
    "-i", "--id", help="Reducer ID", default='0'
)
parser.add_argument(
    "-k", help="Number of reducers", default=1, type=int
)
parser.add_argument(
    "-n", help="Number of splitters, mappers and shuffles", default=1, type=int
)


args = parser.parse_args()
n_iter = 0

channel.queue_declare("shuffle_"+args.id)

pair = {}
method_frame = 0
reduced_pairs = {}
while not method_frame:
    method_frame, header_frame, body = channel.basic_get(queue="shuffle_"+args.id,
                                                         auto_ack=False)
    if method_frame:
        channel.basic_ack(method_frame.delivery_tag)
        pair = json.loads(body.decode("utf-8"))
        if pair[0] == "!The end!":
            n_iter += 1
        else:
            if pair[0] in reduced_pairs.keys():
                reduced_pairs[pair[0]] += sum(pair[1])
            else:
                reduced_pairs[pair[0]] = sum(pair[1])


while n_iter < args.n:

    method_frame, header_frame, body = channel.basic_get(queue="shuffle_"+args.id,
                                                         auto_ack=False)
    if method_frame:
        channel.basic_ack(method_frame.delivery_tag)
        pair = json.loads(body.decode("utf-8"))
        if pair[0] == "!The end!":
            n_iter += 1
        else:
            if pair[0] in reduced_pairs.keys():
                reduced_pairs[pair[0]] += sum(pair[1])
            else:
                reduced_pairs[pair[0]] = sum(pair[1])

#print(args.id, "Unique words at all:", len(reduced_pairs.keys()))
#print(args.id, "Words at all:", sum(reduced_pairs.values()))
#if 'forrest' in reduced_pairs.keys(): print(args.id, "\"Forrest\" count:", reduced_pairs['forrest'])
#if 'a' in reduced_pairs.keys(): print(args.id, "\"A\" count:", reduced_pairs['a'])
send(reduced_pairs)
