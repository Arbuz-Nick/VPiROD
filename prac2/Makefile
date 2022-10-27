all: make_server make_replicas make_client

make_replicas:
	ttab -w "sleep 2 ; cd /Users/anr/Desktop/YPiROD/VPiROD/prac2 ; python3 starter.py -k $(k) -p $(p)"

make_server:
	brew services start rabbitmq

make_client:
	ttab -w "sleep 2 ; python3 client.py"

make stop:
	brew services stop rabbitmq

make restart:
	brew services restart rabbitmq