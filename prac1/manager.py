from multiprocessing import managers
import pika, json, sys, os

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()


def turn_off_dk(dk_cnt):
    for dk_number in range(dk_cnt):
        exchange = "manager-dk"
        queue_name = "in_" + str(dk_number)
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.basic_publish(exchange=exchange, routing_key=queue_name, body="end")


class Manager:
    spliting = []
    letter = ""

    def get_spliting(self):
        print("Manager: Waiting for spliting from ETL")
        consumer_tag = "manager"
        exchange = "etl-manager"
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        queue_name = "manager"
        channel.queue_declare(queue=queue_name, exclusive=True)
        channel.queue_bind(exchange=exchange, queue=queue_name)
        print("Manager: Connection created")
        
        def handler(ch, method, properties, body):
            print("Manager: Got message")
            self.spliting = json.loads(body.decode("utf-8"))
            print("Manager:", self.spliting)
            channel.basic_cancel(consumer_tag=consumer_tag)

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=handler,
            auto_ack=True,
            consumer_tag=consumer_tag,
            exclusive=False,
        )
        print("Manager: Start consuming")
        
        channel.start_consuming()

    def request_streets(self, dk_number):
        exchange = "manager-dk"
        queue_name = "in_" + str(dk_number)
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.basic_publish(
            exchange=exchange, routing_key=queue_name, body=self.letter
        )

    def send_street(self, street):
        exchange = "manager-client"
        queue_name = "out_call-center"
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.basic_publish(exchange=exchange, routing_key=queue_name, body=street)

    def waiting_for_data(self, dk_number):
        print("Manager:: Start waiting")
        exchange = "dk-manager"
        queue_name = "out_" + str(dk_number)
        consumer_tag = "manager_w_dk"
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.queue_declare(queue=queue_name)
        channel.queue_bind(exchange=exchange, queue=queue_name)

        def handler(ch, method, properties, body):
            print("Manager:: Got " + body.decode("utf-8"))
            self.send_street(body.decode("utf-8"))
            if body.decode("utf-8") == "end":
                channel.basic_cancel(consumer_tag=consumer_tag)

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=handler,
            auto_ack=True,
            consumer_tag=consumer_tag,
            exclusive=False,
        )
        print("Manager:: Start consuming with dk")
        channel.start_consuming()

    def get_letter(self):
        exchange = "client-manager"
        queue_name = "in_call-center"
        consumer_tag = "manager_w_clients"
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.queue_declare(queue=queue_name)
        channel.queue_bind(exchange=exchange, queue=queue_name)

        def handler(ch, method, properties, body):
            self.letter = body.decode("utf-8").upper()
            if body.decode("utf-8") == "end":
                print("Manager:: Got end")
                turn_off_dk(len(self.spliting))
                print("Manager:: Working day is over.")
                try:
                    sys.exit(0)
                except SystemExit:
                    os._exit(0)
            print("Manager:: Got message: " + self.letter)
            channel.basic_cancel(consumer_tag=consumer_tag)

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=handler,
            auto_ack=True,
            consumer_tag=consumer_tag,
            exclusive=True,
        )
        print("Manager:: Start consuming with clients")
        channel.start_consuming()

    def work_with_clients(self):
        print("Manager:: Start work with client")
        while True:
            self.get_letter()
            has_letter = False
            for i in range(0, len(self.spliting)):
                if self.letter in self.spliting[i]:
                    has_letter = True
                    print("Manager:: request_streets")
                    self.request_streets(i)
                    print("Manager:: waiting_for_data")
                    self.waiting_for_data(i)

            if not has_letter:
                print("Manager: No such latter")
                channel.exchange_declare(
                    exchange="manager-client", exchange_type="direct"
                )
                channel.basic_publish(
                    exchange="manager-client",
                    routing_key="out_call-center",
                    body="No such letter",
                )

    def notice_client(self):
        print("Manager: Notice client")
        channel.exchange_declare(exchange="manager-client", exchange_type="direct")
        channel.basic_publish(
            exchange="manager-client",
            routing_key="out_call-center",
            body="I'm ready!",
        )

    def working(self):

        self.get_spliting()
        self.notice_client()
        self.work_with_clients()


try:
    manager = Manager()

    manager.working()
except KeyboardInterrupt:
    print("Manager:: Working day is over.")
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
