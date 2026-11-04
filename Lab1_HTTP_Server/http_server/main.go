package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"slices"
)

// limitClients is an http handler wrapper to ensure that the maxClients
// is not exceeded for the provided handler.
// Example taken from https://eli.thegreenplace.net/2019/on-concurrency-in-go-http-servers/
func limitClients(f http.HandlerFunc, maxClients int) http.HandlerFunc {
  // Counting semaphore using a buffered channel
  sema := make(chan struct{}, maxClients)

  return func(w http.ResponseWriter, req *http.Request) {
    sema <- struct{}{}
    defer func() { <-sema }()
    f(w, req)
  }
}

// serveFile is the main HTTP handler for the server that involves
// serving a file on GET requests and loading a file for POST requests
func serveFile(w http.ResponseWriter, req *http.Request) {
	contentType := req.Header.Get("Content-type")
	allowedTypes := []string{"text/html", "text/plain", "image/gif", "image/jpeg", "image/png", "text/css"}
	if slices.Contains(allowedTypes, contentType) {
		if req.Method == "POST" {
			// Collect the file that was sent in this request and write to disk
		} else if req.Method == "GET" {
			// Read the requested file from disk and serve the file
			// serve a 404 if the file cannot be found
		} else {
			http.Error(w, "Method not implemented", http.StatusNotImplemented)
		}
	} else {
		http.Error(w, "Content-type not allowed", http.StatusBadRequest)
	}
}

func main() {
	// NOTE: this should also take first arg if -port is not specified
	port := flag.Int("port", 5555, "the port number to start the listener on")

	flag.Parse()

	// NOTE: this might not be completely correct
	// ListenAndServe automagically spawns a new goroutine for all clients
	// so we are technically still creating the goroutines but blocking them
	// immediately. This may satisfy the requirements but idk.
	http.HandleFunc("/", limitClients(serveFile, 10))

	// Start the listener on the specified port
	fmt.Println("Starting listener on port", *port)
	portString := fmt.Sprintf(":%d", *port)
	log.Fatal(http.ListenAndServe(portString, nil))
}
