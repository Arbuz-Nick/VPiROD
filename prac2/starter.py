#!/usr/bin/python3


import sys
import os
import argparse
import subprocess
import pika


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-k", help="Number of random sends in epidemia protocol", default=1, type=int
    )
    parser.add_argument(
        "-p", "--proc", help="Number of process for data keepers", default=1, type=int
    )
    args = parser.parse_args()

    log_file = open("./log.txt", "w+")
    replica = []
    #subprocess.Popen(["export PATH=$PATH:/usr/local/sbin"])
    #rabbitmq_server = subprocess.Popen(["rabbitmq-server"], stdout=log_file)

    for i in range(args.proc):
        replica.append(
            subprocess.Popen(
                ["python3", "./replica.py", "-i",
                    str(i), "-k", str(args.k), "-n", str(args.proc)]
            )
        )
#    client = subprocess.Popen(["python", "./client.py"])
    replica[0].wait()
    print(replica[0])
#    client.wait()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
