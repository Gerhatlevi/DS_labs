package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
)

func extensionToContentType() map[string]string {
	return map[string]string{
		"html": "text/html",
		"txt": "text/plain",
		"gif": "image/gif",
		"jpeg": "image/jpeg",
		"jpg": "image/jpeg",
		"css": "text/css",
	}
}

// bodyWrapper is a helper function that takes a string and wraps it into
// an io.ReadCloser type which is required for the http.Response body
// used to provide a Close() function to the bytes buffer object which does
// not provide such a function
func bodyWrapper(s string) io.ReadCloser {
	return ioutil.NopCloser(bytes.NewBuffer([]byte(s)))
}

// handleConn is the main HTTP handler for the server that involves
// serving a file on GET requests and storing a file for POST requests
func handleConn(conn net.Conn) {
	// parse the connection as an HTTP request to simplify logic
	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)

	// setup the response object to simplify returning
	res := http.Response{}
	res.Proto = req.Proto
	res.ProtoMajor = req.ProtoMajor
	res.ProtoMinor = req.ProtoMinor
	res.Header = make(map[string][]string)

	if err != nil {
		res.StatusCode = http.StatusBadRequest
		res.Status = "400 Bad Request"
		res.Write(conn)
		conn.Close()
		return
	}

	// get the extension of the requested file for lookup for Content-type
	uri := req.RequestURI
	uriList := strings.Split(uri, ".")
	ext := uriList[len(uriList)-1]
	contentType, ok := extensionToContentType()[ext]
	fmt.Println("got URI", uri)

	// only handle this request if it is a supported file extension
	if ok {
		if req.Method == "POST" {
			// Collect the file that was sent in this request and write to disk
			res.StatusCode = http.StatusCreated
		} else if req.Method == "GET" {
			// Read the requested file from disk and serve the file
			// serve a 404 if the file cannot be found
			res.StatusCode = http.StatusOK
			res.Header.Set(http.CanonicalHeaderKey("Content-type"), contentType)
			res.Body = bodyWrapper("henlo\n")
		} else {
			res.StatusCode = http.StatusNotImplemented
		}
	} else {
		res.StatusCode = http.StatusBadRequest
		res.Status = "400 Bad Request"
	}

	// write the http.Response object to the connection
	res.Write(conn)
	// properly close the connection once the file has been serviced
	conn.Close()
}

func main() {
	// NOTE: this should also take first arg if -port is not specified
	port := flag.Int("port", 5555, "the port number to start the listener on")

	flag.Parse()

	// Start the listener on the specified port
	fmt.Println("Starting listener on port", *port)
	portString := fmt.Sprintf(":%d", *port)
	ln, err := net.Listen("tcp", portString)
	if err != nil {
		log.Printf("Failed to start listener: %s", err)
		return
	}

	//maxConcurrent := 10

	// handle incoming connections and spawn off additional goroutines
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %s", err)
			continue
		}

		// create a goroutine to handle this connection
		// NOTE: need to add in a check here to ensure we aren't exceeding maxConcurrent
		go handleConn(conn)
	}
}
