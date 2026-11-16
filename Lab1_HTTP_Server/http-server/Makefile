default: server

server: main.go
	go build -o http_server

test: clean server
	./http_server 5555 &
	sleep 1
	./test.sh
	make clean

clean:
	# kill the existing http server if found
	kill $(shell ps aux | grep '[h]ttp_server 5555' | awk '{print $$2}') || echo "No server started, skipping..."
	rm http_server || echo "No http_server binary found, skipping..."
	rm *.txt || echo "No txt files found, skipping..."
	rm -r files || echo "No files folder found, skipping..."
