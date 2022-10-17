import queue
import osmium as osm
import pandas as pd
import sys
import numpy as np
import pika
import json
import time
import re


###                              ###
#   Открываем и парсим .oms файл   #
###                              ###
class OSMHandler(osm.SimpleHandler):
    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.osm_data = []

    def tag_inventory(self, elem, elem_type):
        for tag in elem.tags:
            self.osm_data.append(
                [
                    tag.k,
                    tag.v,
                ]
            )

    def node(self, n):
        self.tag_inventory(n, "node")

    def way(self, w):
        self.tag_inventory(w, "way")

    def relation(self, r):
        self.tag_inventory(r, "relation")


osmhandler = OSMHandler()
print("ETL: Start reading")
start = time.process_time()
# scan the input file and fills the handler list accordingly
osmhandler.apply_file("map.osm" if len(sys.argv) <= 1 else sys.argv[2])
stop = time.process_time()
execution_time = stop - start


print("ETL: File readed in", execution_time, "seconds")
# transform the list into a pandas DataFrame
data_colnames = [
    "tagkey",
    "tagvalue",
]
df_osm = pd.DataFrame(osmhandler.osm_data, columns=data_colnames)
###                    ###
#   Делаем список улиц   #
###                    ###
streets = df_osm[df_osm.tagkey == "addr:street"]["tagvalue"].unique()
print(len(streets))
streets = list(
    map(
        lambda x: "".join(
            ch
            for ch in x.replace("улица", "")
            .replace("проезд", "")
            .replace("шоссе", "")
            .replace("бульвар", "")
            .replace("переулок", "")
            .replace("проспект", "")
            .replace("переулок", "")
            .strip()
            if (ch.isalnum() or ch.isspace())
        ),
        streets,
    )
)
new_streets = [
    street
    for street in streets
    if street[0] == street[0].upper() and not street[0].isspace()
]
streets = new_streets
###                                                                 ###
#   Делаем разбиение по первым буквам улиц на количество хранителей,  #
#   указанное в параметрах командной строки                           #
###                                                                 ###
first_letters = sorted(list(set(streets[i][0] for i in range(0, len(streets)))))
spliting = [
    list(i)
    for i in np.array_split(
        first_letters, 1 if len(sys.argv) == 1 else int(sys.argv[1])
    )
]
print("ETL:", spliting)

###                             ###
#   Отправляем данные в очереди   #
###                             ###
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
out_ch = connection.channel()
out_ch.exchange_declare(exchange="etl-dk", exchange_type="direct")
print("ETL: Start sending to Data Keepers")
for i in range(0, 1 if len(sys.argv) == 1 else int(sys.argv[1])):
    queue_name = "in_" + str(i)
    for street in streets:
        if street[0] in spliting[i]:
            out_ch.basic_publish(exchange="etl-dk", routing_key=queue_name, body=street)
    out_ch.basic_publish(exchange="etl-dk", routing_key=queue_name, body="end")
print("ETL: Done")
print("ETL: Send ready status to Manager")
out_ch.exchange_declare(exchange="etl-manager", exchange_type="direct")
out_ch.basic_publish(
    exchange="etl-manager", routing_key="manager", body=json.dumps(spliting)
)
print("ETL: Finish")
connection.close()
