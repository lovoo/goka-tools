# Goka Tools [![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

Goka Tools is a collection of tools that integrate with [Goka](https://github.com/lovoo/goka), a distributed stream processing library for Apache Kafka written in Go.

## Tools

* BBQ: A package that writes messages from a Kafka topic directly into BigQuery.
* Tailer: A package used for retrieving the last n messages from a given topic.
* DotGen: A package for generating DOT files that describe a Goka GroupGraph.

## Installation

You can install Goka Tools by running the following command:

``$ go get -u github.com/lovoo/goka-tools``

## How to contribute

Contributions are always welcome.
Please fork the repo, create a pull request against master, and be sure tests pass.
See the [GitHub Flow](https://guides.github.com/introduction/flow) for details.