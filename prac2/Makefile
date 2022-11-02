all: make_server make_replicas make_client

replicas:
	ttab -w "sleep 2 ; cd /Users/anr/Desktop/YPiROD/VPiROD/prac2 ; python3 starter.py -k $(k) -p $(p)"

server:
	brew services start rabbitmq

client:
	ttab -w "sleep 2 ; python3 client.py"

stop:
	brew services stop rabbitmq

restart:
	brew services restart rabbitmq