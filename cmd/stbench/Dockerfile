
FROM golang:1.16

# RUN apt-get update
# RUN apt-get install iproute2 -y
# RUN apt-get install build-essential -y
WORKDIR /go/src/github.com/lovoo/goka-tools

# copy and install dependencies
COPY go.mod .
COPY go.sum .
RUN go mod download -x

# copy rest and compile
COPY . .
RUN go build -o /stbench cmd/stbench/main.go
