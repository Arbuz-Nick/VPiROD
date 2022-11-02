from multiprocessing import managers

import pika
import json
import argparse
import sys
import os
from random import randint
import time

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

    def get_val(self, exchange="main", queue="r_" + str(args.id)):
        self.consumer_tag = queue
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        queue_name = self.consumer_tag
        channel.queue_declare(queue=queue_name, exclusive=False)
        channel.queue_bind(exchange=exchange, queue=queue_name)

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.handler,
            auto_ack=True,
            consumer_tag=self.consumer_tag,
            exclusive=False
        )
        #channel.basic_qos(prefetch_count=1, global_qos=False)
        channel.start_consuming()

    def notify_main(self):
        exchange = "to_main"
        routing_key = "notify"
        # print("Replica {}: Notify".format(
        #    str(args.id)))
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=json.dumps({"from": "r_" + str(args.id), "data": self.new_val}))

    def epidemia(self):
        i = 0
        while i < args.k:
            i += 1
            if (args.rnum > 2) or (args.id == 0):
                id = randint(1, args.rnum - 1)
                if id == args.id:
                    i -= 1
                    continue
                #print("From", args.id, "to", id)
                self.send(id)
                # time.sleep(1)

    def save_new_val(self):
        if self.new_val["name"] in self.data.keys():
            self.data[self.new_val["name"]][0] = self.new_val["id"]
            self.data[self.new_val["name"]][1] = self.new_val["val"]
        else:
            self.data[self.new_val["name"]] = [self.new_val["id"],
                                               self.new_val["val"]]

    def working(self):
        while True:
            self.get_val(exchange="main")
            # print("Replica {}: Got value:".format(
            #    str(args.id)), self.new_val)
            # print("Replica {}:".format(
            #     str(args.id)), "\n\tnew_val:", self.new_val, "\n\tdata:", self.data)
            if (self.new_val["name"] not in self.data.keys()) or (self.data[self.new_val["name"]][0] < self.new_val["id"]):
                self.notify_main()
                self.save_new_val()
                if args.rnum > 1:
                    self.epidemia()


class MainReplica(Replica):

    ids = dict()

    def get_notify(self):
        method_frame, header_frame, body = channel.basic_get(queue="notify",
                                                             auto_ack=False)
        if method_frame:
            #print(method_frame, header_frame, json.loads(body.decode("utf-8")))
            channel.basic_ack(method_frame.delivery_tag)
            self.new_val = json.loads(body.decode("utf-8"))
        return method_frame

    def waiting(self):
        replicas = dict(("r_" + str(i), 1) for i in range(1, args.rnum))
        exchange = "to_main"
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        queue_name = "notify"
        channel.queue_declare(queue=queue_name, exclusive=False)
        channel.queue_bind(exchange=exchange, queue=queue_name)

        i = 0
        while sum(replicas.values()):
            i += 1
            #self.get_val(exchange="to_main", queue="notify")
            time.sleep(0.03)
            method_frame = self.get_notify()
            if method_frame:
                replicas[self.new_val["from"]] = 0
                # print("Main Got", self.new_val, '\n\t left:', [
                #    key for key in replicas.keys() if replicas[key] > 0])
            if i > 100:
                i = 0
                self.new_val = self.new_val["data"]
                [self.send(int(key.replace("r_", "")))
                 for key in replicas.keys() if replicas[key] > 0]

    def get_id(self, name):
        if name in self.data.keys():
            return self.data[name][0] + 1
        return 0

    def working(self):
        while True:
            self.get_val(exchange="client-replica", queue="income")
            #print("Got:", self.new_val)
            if self.new_val["from"] == "client":
                self.new_val = {
                    "id": 0, "name": self.new_val["data"]["name"], "val": self.new_val["data"]["val"]}
                self.new_val["id"] = self.get_id(self.new_val["name"])
                val = self.new_val
                self.save_new_val()
                # print("Saved")
                if args.rnum > 1:
                    self.epidemia()
                #print("Start waiting")
                self.waiting()
                print("MainReplica:", val)
            else:
                print("Wrong place")


try:
    if args.id > 0:
        replica = Replica()
    else:
        replica = MainReplica()
    replica.working()

except KeyboardInterrupt:
    print("Replica {}: Working day is over.".format(
        str(args.id)))
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
