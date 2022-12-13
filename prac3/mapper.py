import argparse
import pika
from os import walk, listdir
import os
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))
channel = connection.channel()


def send(str):
    exchange = "mapper-shuffle"
    routing_key = "mapper_" + args.id
    channel.exchange_declare(exchange=exchange, exchange_type="direct")
    channel.queue_declare(routing_key)
    channel.queue_bind(queue=routing_key, exchange=exchange)
    channel.basic_publish(
        exchange=exchange, routing_key=routing_key, body=json.dumps((str, 1))
    )


parser = argparse.ArgumentParser()
parser.add_argument(
    "-i", "--id", help="Mapper ID", default='0'
)

args = parser.parse_args()

str = ''
method_frame = 0

channel.queue_declare("splitter_" + args.id)

while not method_frame:
    method_frame, header_frame, body = channel.basic_get(queue="splitter_" + args.id,
                                                         auto_ack=False)
    if method_frame:
        channel.basic_ack(method_frame.delivery_tag)
        str = body.decode("utf-8")
        #print(args.id, "Got:", str)


while str != '!The end!':

    method_frame, header_frame, body = channel.basic_get(queue="splitter_" + args.id,
                                                         auto_ack=False)
    if method_frame:
        channel.basic_ack(method_frame.delivery_tag)
        str = body.decode("utf-8")
        if str != '!The end!':
            words = str.split()
            map = []
            for word in words:
                if len(word) <= 20:
                    send(word)
send("!The end!")
