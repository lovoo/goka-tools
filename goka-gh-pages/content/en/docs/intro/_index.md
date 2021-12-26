+++
title="Intro"
+++


Goka is a library, if you just want to use it, add it to your project with
```bash
$ go get -u github.com/lovoo/goka
```


If you are new to goka, best clone the repo to check out the examples. They come with a ready-to-go kafka-cluster using `docker-compose`.

```bash
cd $GOPATH/src
mkdir -p github.com/lovoo/
cd github.com/lovoo/
git clone https://github.com/lovoo/goka.git
# or
git clone git@github.com:lovoo/goka.git
# or
gh repo clone lovoo/goka


## go to the examples
cd goka/examples

## run a cluster
make start
# wait some seconds - kafka needs time to warm up.
# run the first examples
go run 1-simplest/main.go
# Output:
# 2021/10/12 21:33:51 message emitted
# 2021/10/12 21:33:54 key = some-key, counter = 1, msg = some-value

```


Goka relies on [Sarama](https://github.com/Shopify/sarama) to perform the actual communication with Kafka, which offers many configuration settings. The config is documented [here](https://godoc.org/github.com/Shopify/sarama#Config).

In most cases, you need to modify the config, e.g. to set the Kafka Version.

```golang
cfg := goka.DefaultConfig()
cfg.Version = sarama.V2_4_0_0
goka.ReplaceGlobalConfig(cfg)
```