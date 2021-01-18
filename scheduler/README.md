# Goka Scheduler

This tool implements a distributed scheduler fully based on goka.

Use it to delay events or throttle events. The basic input is an "order", that is
either a Delay or a Throttle.
Each order contains a "payload", i.e. a target topic, key and message that are being send
when the delay is expired.

## Delay 

Send event after certain delay, each order is unique, so there's no throttling

## Throttle

Send event after a delay, but intermediate orders with the same key are ignored.
A key is made of
* target topic
* target key
* a variant that allows to "seed" the message to avoid unwanted collisions and thus throttling of events.

There are two versions:
* ThrottleFirst will send the first order's payload and will ignore following orders
* ThrottleLast will send the last order's payload, so it will keep updating the target payload.

## Testing

The scheduler cannot be tested using goka's builtin `tester`-feature, so instead there are tests that require a running kafka-cluster. The easiest way is to run the kafka-cluster using `docker-compose`, provided in `goka/examples`. Then run `make full-test` to include the integration-tests that need kafka.

## Example

TODO

## Client usage 

TODO

## Intervallers