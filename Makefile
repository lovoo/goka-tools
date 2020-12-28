test:
	GOPATH=$(GOPATH) go test ./...

stbench:
	CGO_ENABELD=1 go build -o build/stbench cmd/stbench/main.go

	docker build -t stbench -f cmd/stbench/Dockerfile .
	-docker rm stbench
	docker run --rm -it --name stbench -v /home/franz/docker:/out --cpus=1 --memory=500m --device-write-bps=/dev/dm-0:10mb --device-read-bps=/dev/dm-0:50mb stbench /stbench --keys=1000000 --path /out/devicetracker --reuse

docker-stats:
	docker run --rm	-p 8080:8080 --name stats\
					--volume=/var/run/docker.sock:/var/run/docker.sock:ro \
					-e STATS_UPDATE_INTERVAL=1 virtualzone/docker-container-stats

