run:
	rm -r data
	go run example/*.go

run-db:
	go run example/*.go

run-bench:
	rm -r tmp-*
	go run benchmark/*.go -db mewdb
	go run benchmark/*.go -db rosedb
	go run benchmark/*.go -db leveldb
	rm -r tmp-*

test-cover:
	go test -race -coverprofile=coverage.txt -covermode=atomic
	go tool cover -html=coverage.txt -o coverage.html