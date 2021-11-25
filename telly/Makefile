

start-rtdb:
	docker run --rm -d -v "/tmp/goka-tools-rethinktest:/data" -p 5985:8080 -p 28015:28015 --name rethink1 rethinkdb

stop-rtdb:
	docker stop rethink1