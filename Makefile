run:
	go run example/*.go

test-cover:
	go test -race -coverprofile=coverage.txt -covermode=atomic
	go tool cover -html=coverage.txt -o coverage.html