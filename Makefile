ifeq ($(shell uname -s),Darwin)
DOCKER_USERGROUP := $(shell id -u):$(shell id -g)
else
DOCKER_USERGROUP := $(shell id --user):$(shell id --group)
endif


test:
	GOPATH=$(GOPATH) go test ./...

build-bench:
	CGO_ENABELD=1 go build -o build/stbench cmd/stbench/main.go
	docker build -t stbench -f cmd/stbench/Dockerfile .

run-leveldb:
	docker run --rm -it --user $(DOCKER_USERGROUP) --name stbench -v /home/franz/docker:/out --cpus=1 --memory=500m --device-write-iops=/dev/dm-0:300 --device-read-iops=/dev/dm-0:300 --device-write-bps=/dev/dm-0:5mb \
			--device-read-bps=/dev/dm-0:5mb \
			stbench /stbench --keys=1000000 \
			 --path /out/devicetracker --storage=leveldb \
			--clear \
			--stats /out/stats/leveldb-1m-stats
	
run-pogrep:
	docker run --rm -it --user $(DOCKER_USERGROUP) --name stbench -v /home/franz/docker:/out/ --cpus=1 --memory=500m --device-write-iops=/dev/dm-0:300 --device-read-iops=/dev/dm-0:300 --device-write-bps=/dev/dm-0:5mb \
			--device-read-bps=/dev/dm-0:5mb \
			stbench /stbench --keys=1000000 \
			 --path /out/pogrep --storage=pogrep \
			--duration=120 \
			--stats /out/stats/pogrep-1m-stats-reuse
	


docker-stats:
	docker run --rm	-p 8080:8080 --name stats\
					--volume=/var/run/docker.sock:/var/run/docker.sock:ro \
					-e STATS_UPDATE_INTERVAL=1 virtualzone/docker-container-stats



