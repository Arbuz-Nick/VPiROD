import pika, sys, os

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()


class Client:
    letters = []

    def get_letters(self):
        print("For what letter should I ask?")
        self.letters = input()

    def send(self, letter):
        exchange = "client-manager"
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.basic_publish(exchange=exchange, routing_key="in_call-center", body=letter)

    def get_streets(self, base_letter):
        exchange = "manager-client"
        consumer_tag = "client"
        queue_name = "out_call-center"
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.queue_declare(queue=queue_name)
        channel.queue_bind(exchange=exchange, queue=queue_name)

        def handler(ch, method, properties, body):
            street = body.decode("utf-8")
            if street == "No such letter":
                print("There is no letter " + base_letter)
                channel.basic_cancel(consumer_tag=consumer_tag)
            elif street == "end":
                print("No more streets")
                channel.basic_cancel(consumer_tag=consumer_tag)
            else:
                print("It's street: " + street)

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=handler,
            auto_ack=True,
            consumer_tag=consumer_tag,
            exclusive=True,
        )
        print("Start consuming")
        channel.start_consuming()

    def work_with_manager(self):
        for letter in self.letters:
            self.send(letter)
            self.get_streets(letter)
    
    def waiting_for_manager(self):
        exchange = "manager-client"
        consumer_tag = "client"
        queue_name = "out_call-center"
        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.queue_declare(queue=queue_name)
        channel.queue_bind(exchange=exchange, queue=queue_name)
        print("Connection created")

        def handler(ch, method, properties, body):
            print("Here you are!", body.decode("utf-8"))
            channel.basic_cancel(consumer_tag=consumer_tag)

        channel.basic_consume(
            queue=queue_name,
            on_message_callback=handler,
            auto_ack=True,
            consumer_tag=consumer_tag,
            exclusive=True,
        )
        print("Waiting for manager")
        channel.start_consuming()

    def do_stuff(self):
        print("Press Ctrl-C to finish.")
        self.waiting_for_manager()
        while True:
            self.get_letters()
            self.work_with_manager()


client = Client()
try:
    client.do_stuff()
except KeyboardInterrupt:
    #exchange = "client-manager"
    #channel.exchange_declare(exchange=exchange, exchange_type="direct")
    #channel.basic_publish(exchange=exchange, routing_key="in_call-center", body="end")
    #connection.close()
    print("Goodbye! Have a nice day!")
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
