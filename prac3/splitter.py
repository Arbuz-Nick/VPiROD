import argparse
import pika
from os import walk, listdir
import os

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

parser = argparse.ArgumentParser()
parser.add_argument(
    "-i", "--id", help="Splitter ID", default='0'
)
parser.add_argument(
    "-t", "--txt", help="Path to dir with texts", default="./"
)
args = parser.parse_args()


def send(str):
    exchange = "splitter-mapper"
    routing_key = "splitter_" + args.id
    channel.exchange_declare(exchange=exchange, exchange_type="direct")
    channel.queue_declare(routing_key)
    channel.queue_bind(queue=routing_key, exchange=exchange)
    channel.basic_publish(
        exchange=exchange, routing_key=routing_key, body=str
    )


# parser.add_argument(
#    "-k", help="Number of reducers", default=1, type=int
# )
# parser.add_argument(
#    "-n", help="Number of splitters, mappers and shuffles", default=1, type=int
# )

txt_files = []
for filename in listdir(args.txt):
    if os.path.splitext(filename)[1] == '.txt':
        txt_files.append(filename)

for file_name in txt_files:
    file = open(os.path.join(args.txt, file_name))
    s = file.readline()
    while s:
        s = ''.join([i.lower() for i in s if i.isalpha() or i.isspace()])
        if s.split() != []:
            send(s)
            #print(args.id+": Sended:", s)
        s = file.readline()
send("!The end!")
