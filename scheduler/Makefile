proto:
	protoc -I=. --go_out=paths=source_relative:. order.proto



full-test:
	go test ./... --args -integration-test