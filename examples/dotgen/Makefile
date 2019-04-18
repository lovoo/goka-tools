# you need a running local kafka for the processors to shortly start up and get the group graph
start-kafka:
	# stop old versions of the container
	-docker stop kafka-cluster1
	-docker rm kafka-cluster1
	docker run --name=kafka-cluster1 -d  -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=127.0.0.1 --env NUM_PARTITIONS=10 --env ADVERTISED_PORT=9092 spotify/kafka

generate-dot:
	go run main.go --output=generated.dot
	cat generated.dot | docker container run --rm -i vladgolubev/dot2png > graph.png
	