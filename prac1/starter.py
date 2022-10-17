#!/usr/bin/python3


import sys, os, argparse, subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o", "--osm", help="Filename for .osm input", default="map.osm"
    )
    parser.add_argument(
        "-p", "--proc", help="Number of process for data keepers", type=int, default=1
    )
    args = parser.parse_args()

    print(args)
    log_file = open("./log.txt", "w+")
    data_keeper = []

    manager = subprocess.Popen(["python", "./manager.py"])#, stdout=log_file)
    for i in range(args.proc):
        data_keeper.append(
            subprocess.Popen(["python", "./data_keeper.py", str(i)], stdout=log_file)
        )
    ETL = subprocess.Popen(
        ["python", "./ETL.py", str(args.proc), args.osm], stdout=log_file
        )
    client = subprocess.Popen(["python", "./client.py"])
    manager.wait()
    print(manager)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
