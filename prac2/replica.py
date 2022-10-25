from multiprocessing import managers

import pika
import json
import argparse
import sys
import os
from random import randint

from torch import wait

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))
channel = connection.channel()
parser = argparse.ArgumentParser()
parser.add_argument(
    "-k", help="Number of random sends in epidemia protocol", default=1, type=int
)
parser.add_argument(
    "-i", "--id", help="Replica ID", type=int, default=0
)
parser.add_argument(
    "-n", "--rnum", help="Count of replica", type=int, default=1
)
args = parser.parse_args()

print(args)


class Replica():
    data = dict()

    def handler(self, ch, method, properties, body):
        self.new_val = json.loads(body.decode("utf-8"))
        channel.basic_cancel(consumer_tag=self.consumer_tag)

    def send(self, id):
        exchange = "main"
        routing_key = "r_" + str(id)
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=json.dumps(
                self.new_val)
        )

    def get_val(self, exchange="main"):
        self.consumer_tag = "r_" + str(args.id)
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        queue_name = self.consumer_tag
        channel.queue_declare(queue=queue_name, exclusive=False)
        channel.queue_bind(exchange=exchange, queue=queue_name)

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.handler,
            auto_ack=True,
            consumer_tag=self.consumer_tag,
            exclusive=False,
        )
        channel.start_consuming()

    def notify_main(self):
        exchange = "main"
        routing_key = "r_0"
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=json.dumps({"from": "r_" + str(args.id), "data": self.new_val}))

    def epidemia(self):
        i = 0
        while i < args.k:
            if (args.rnum > 2) or (args.id == 0):
                id = randint(1, args.rnum - 1)
                if id == args.id:
                    continue
                self.send(id)
            i += 1

    def save_new_val(self):
        if self.new_val["name"] in self.data.keys():
            if self.data[self.new_val["name"]][0] < self.new_val["id"]:

                self.data[self.new_val["name"]][0] = self.new_val["id"]
                self.data[self.new_val["name"]][1] = self.new_val["val"]
        else:
            self.data[self.new_val["name"]] = [
                self.new_val["id"], self.new_val["val"]]

    def working(self):
        while True:
            self.get_val(exchange="main")
            self.notify_main()
            print("Replica {}: Got value:".format(
                str(args.id)), self.new_val)
            self.save_new_val()

            if args.rnum > 1:
                self.epidemia()


class MainReplica(Replica):

    ids = dict()

    def waiting(self):
        replicas = dict(("r_" + str(i), 1) for i in range(1, args.rnum))
        while sum(replicas.values()):
            self.get_val(exchange="main")
            replicas[self.new_val["from"]] = 0

    def get_id(self, name):
        if name in self.ids.keys():
            self.ids[name] += 1
        else:
            self.ids[name] = 0
        return self.ids[name]

    def working(self):
        while True:
            self.get_val(exchange="client-replica")
            if self.new_val["from"] == "client":
                self.new_val = {"id": 0, "name": self.new_val["data"]["name"], "val": self.new_val["data"]["val"]}
                self.new_val["id"] = self.get_id(self.new_val["name"])
                val = self.new_val
                self.save_new_val()
                if args.rnum > 1:
                    self.epidemia()
                self.waiting()
                print("MainReplica:", val)


try:
    if args.id > 0:
        replica = Replica()
    else:
        replica = MainReplica()
    replica.working()

except KeyboardInterrupt:
    print("Manager:: Working day is over.")
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
