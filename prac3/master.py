
import sys
import os
import argparse
import subprocess
import pika
from os import walk, listdir
from os.path import isfile, join
import json

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))
channel = connection.channel()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t", "--txt", help="Path to dir with texts", default="./txt"
    )
    parser.add_argument(
        "-k", help="Number of reducers", default=1, type=int
    )
    parser.add_argument(
        "-n", help="Number of splitters, mappers and shuffles", default=1, type=int
    )
    args = parser.parse_args()

    proc = subprocess.Popen(["rm", "*_log.txt"])
    proc.wait()

    for i in range(args.n):
        proc = subprocess.Popen(["rm", "-r", "M"+str(i+1)])
        proc.wait()
        proc = subprocess.Popen(["mkdir", "M"+str(i+1)])
        proc.wait()

    txt_files = []
    for filename in listdir(args.txt):
        if os.path.splitext(filename)[1] == '.txt':
            txt_files.append(filename)
    # print(txt_files)

    print("Copying texts")
    txt_num = len(txt_files)
    proc = []
    for i in range(txt_num):
        proc.append(subprocess.Popen(
            ["cp", join(args.txt, txt_files[i]), "M"+str(i % args.n + 1)]))
    for i in range(txt_num):
        proc[i].wait()

    print("Start splitters")
    log = open("splitter_log.txt", "w")
    proc = []
    for i in range(args.n):
        proc.append(subprocess.Popen(
            ["python3", "splitter.py", "-i", str(i), "-t", "M"+str(i + 1)], stdout=log))
    for i in range(args.n):
        proc[i].wait()
    log.close()

    print("Start mappers")
    log = open("mapper_log.txt", "w")
    proc = []
    for i in range(args.n):
        proc.append(subprocess.Popen(
            ["python3", "mapper.py", "-i", str(i)], stdout=log))
    for i in range(args.n):
        proc[i].wait()
    log.close()

    print("Start shuffles")
    proc = []
    for i in range(args.n):
        proc.append(subprocess.Popen(
            ["python3", "shuffle.py", "-i", str(i), "-k", str(args.k)]))
    for i in range(args.n):
        proc[i].wait()

    print("Start resucers")
    proc = []
    for i in range(args.k):
        proc.append(subprocess.Popen(
            ["python3", "reducer.py", "-i", str(i), "-k", str(args.k), "-n", str(args.n)]))
    for i in range(args.k):
        proc[i].wait()

    channel.queue_declare("last_step")

    pairs = {}
    method_frame = 0
    reduced_pairs = {}
    while not method_frame:
        method_frame, header_frame, body = channel.basic_get(queue="last_step",
                                                             auto_ack=False)
        if method_frame:
            channel.basic_ack(method_frame.delivery_tag)
            pairs = json.loads(body.decode("utf-8"))
            for item in pairs.items():
                reduced_pairs[item[0]] = item[1]

    k_iter = 1
    while k_iter < args.k:

        method_frame, header_frame, body = channel.basic_get(queue="last_step",
                                                             auto_ack=False)
        if method_frame:
            channel.basic_ack(method_frame.delivery_tag)
            pairs = json.loads(body.decode("utf-8"))
            k_iter += 1
            for item in pairs.items():
                reduced_pairs[item[0]] = item[1]

    sorted_keys = sorted(reduced_pairs.keys())

    for key in sorted_keys:
        print("<", key, ",", reduced_pairs[key], ">", sep='')

    #print("Unique words at all:", len(reduced_pairs.keys()))
    #print("Words at all:", sum(reduced_pairs.values()))
    #print("\"Forrest\" count:", reduced_pairs['forrest'])
    #print("\"A\" count:", reduced_pairs['a'])


if __name__ == "__main__":
    main()
