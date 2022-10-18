import pika
import sys
import os


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))

channel = connection.channel()


class Client:

    def get_value(self):
        print("Waiting for: <variable>, <value>")
        try:
            self.var, self.val = input(
                "Enter new var/val: ").replace(",", " ").split(maxsplit=1)
        except KeyboardInterrupt:
            print("\nGoodbye! Have a nice day!")
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
        except:
            print("Something wrong with input. Try again.")
            self.get_value()

    def send_value(self):
        exchange = "client-main"
        routing_key = "new_vals"

        channel.exchange_declare(exchange=exchange, exchange_type="direct")
        channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=(self.var+" "+self.val))

    def do_stuff(self):
        print("Press Ctrl-C to finish.")

#        self.waiting_for_manager()
        while True:
            self.get_value()
            self.send_value()


client = Client()
try:
    client.do_stuff()
except KeyboardInterrupt:
    print("Goodbye! Have a nice day!")
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
