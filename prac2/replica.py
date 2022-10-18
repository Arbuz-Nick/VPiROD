from multiprocessing import managers

import pika
import json
import sys
import os

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))
channel = connection.channel()
parser = argparse.ArgumentParser()
parser.add_argument(
    "-k", help="Number of random sends in epidemia protocol", default=1
)
parser.add_argument(
    "-i", "--id", help="Replica ID", type=int, default=1
)
args = parser.parse_args()

print(args)


class Replica():
    data = {}
    cur_version = {}
    cur_

    def get_val(self):
        
    def working(self):
        self.get_val()
        for i in range(args.k):
            self.send()


class MainReplica(Replica):
    data = 0


try:
    if (sys.argv[1]):
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
