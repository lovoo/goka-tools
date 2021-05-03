ifeq ($(shell uname -s),Darwin)
DOCKER_USERGROUP := $(shell id -u):$(shell id -g)
else
DOCKER_USERGROUP := $(shell id --user):$(shell id --group)
endif


test:
	GOPATH=$(GOPATH) go test ./...

build-bench:
	docker build -t stbench -f cmd/stbench/Dockerfile .

STORAGE ?=
run-experiment:
	docker run --rm -it --user $(DOCKER_USERGROUP) -v `pwd`/stbench_eval_out:/out/ \
			--cpus=1 --memory=500m \
			--device-write-iops=/dev/dm-0:300 \
			--device-read-iops=/dev/dm-0:300 --device-write-bps=/dev/dm-0:10mb \
			--device-read-bps=/dev/dm-0:10mb \
			stbench /stbench --keys=2000000 \
			 --path /out/$(STORAGE)/ --storage=$(STORAGE) \
			 --duration=30 \
			--clear \
			--stats /out/$(STORAGE)/stats.csv
	
run-all:
	STORAGE=leveldb $(MAKE) run-experiment
	STORAGE=pogreb-offsetsync $(MAKE) run-experiment
	STORAGE=pogreb-batch-recover $(MAKE) run-experiment
	STORAGE=leveldb2 $(MAKE) run-experiment


docker-stats:
	docker run --rm	-p 8080:8080 --name stats\
					--volume=/var/run/docker.sock:/var/run/docker.sock:ro \
					-e STATS_UPDATE_INTERVAL=1 virtualzone/docker-container-stats

