package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

// High level constants for defining the number of
// maxConCurrent service threads to manage each request
// and defining an outFolder to get created and written to
// for clients writing and reading files.
const (
	maxConCurrent = 10
	outFolder     = "files"
)

type LockManager struct {
	mu    sync.Mutex
	locks map[string]*sync.RWMutex
}

// NewLockManager is used for instantiating the LockManager
// with a new map for fileName (string) -> lock (*sync.RWMutex)
func NewLockManager() *LockManager {
	return &LockManager{locks: make(map[string]*sync.RWMutex)}
}

// ForFile creates a new RWMutex for a file if it does not exist
// yet and returns the existing lock if it does exist. This allows
// better throughput to provide concurrency on a per file basis instead
// of a global mutex for all files.
func (m *LockManager) ForFile(path string) *sync.RWMutex {
	m.mu.Lock()
	defer m.mu.Unlock()

	// if the lock exists already, return it
	if lock := m.locks[path]; lock != nil {
		return lock
	}

	// otherwise, create a new lock for this file
	lock := &sync.RWMutex{}
	m.locks[path] = lock
	return lock
}

// locks is a global lock manager for synchronizing reads and writes
// to uploaded files in the given HTTP endpoints.
var locks = NewLockManager()

// Semaphore is an implementation of a semaphore using go buffered channels
// here a struct{} is used since an empty struct is the smallest size
// and has a width of 0. So this is an efficient mechanism for implementing
// a Semaphore.
// https://go-cookbook.com/snippets/concurrency/semaphores
type Semaphore struct {
	Channel chan struct{}
}

// Acquire is used for adding a single empty struct to the
// Channel and when the semaphore is created, it is made with
// a Buffered Channel. So when the Channel is full, the thread
// is blocked.
func (s *Semaphore) Acquire() {
	s.Channel <- struct{}{}
}

// Release is similar to Acquire but just pops a value out of
// the Buffered Channel to make space for the next blocking
// thread if one exists. Similar to that of a counting semaphore.
func (s *Semaphore) Release() {
	<-s.Channel
}

// create the Semaphore sem as a Buffered Channel with the
// maxConCurrent desired variable. This will be used for blocking
// an additional go routine and have a max maxConCurrent threads
// servicing data at any given time.
var sem = &Semaphore{Channel: make(chan struct{}, maxConCurrent)}

// extensionToContentType is a simple mapping of supported file extensions
// to their respecive Content-Type strings for responses in GET requests.
func extensionToContentType() map[string]string {
	return map[string]string{
		"html": "text/html",
		"txt":  "text/plain",
		"gif":  "image/gif",
		"jpeg": "image/jpeg",
		"jpg":  "image/jpeg",
		"css":  "text/css",
	}
}

// bodyWrapper is a helper function that takes a string and wraps it into
// an io.ReadCloser type which is required for the http.Response body
// used to provide a Close() function to the bytes buffer object which does
// not provide such a function
func bodyWrapper(s string) io.ReadCloser {
	return io.NopCloser(bytes.NewBuffer([]byte(s)))
}

// setHeader is a helper function that abstracts setting the header for the
// http.Request struct to ensure the CanonicalHeaderKey is called properly
func setHeader(res http.Response, s string, v string) {
	res.Header.Set(s, v)
}

// handleConn is the main HTTP handler for the server that involves
// serving a file on GET requests and storing a file for POST requests
func handleConn(conn net.Conn, released *bool) {
	// parse the connection as an HTTP request to simplify logic
	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)

	// setup the response object to simplify returning
	res := http.Response{}
	// always communicate with HTTP/1.0
	res.Proto = "HTTP/1.0"
	res.ProtoMajor = 1
	res.ProtoMinor = 0
	res.StatusCode = 0 // NOTE: this is used for error checking and must get set before sending to client
	res.Header = make(map[string][]string)

	if err != nil {
		res.StatusCode = http.StatusBadRequest
		res.Status = http.StatusText(res.StatusCode)
		res.Write(conn)
		conn.Close()
		return
	}

	uri := req.URL.Path
	queryParams := req.URL.Query()
	fileName, contentType, ok := getFileInfo(uri)

	switch req.Method {
	case "POST":
		if strings.HasPrefix(uri, "/subscribe") {
			HandleSubscribe(queryParams, &res)
		} else if strings.HasPrefix(uri, "/unsubscribe") {
			HandleUnsubscribe(queryParams, &res)
		} else if strings.HasPrefix(uri, "/publish") {
			HandlePublish(queryParams, req, &res)
		} else if ok {
			handlePostFile(fileName, req, &res)
		} else {
			res.StatusCode = http.StatusNotImplemented
		}
	case "GET":
		if strings.HasPrefix(uri, "/events") {
			HandleEvents(queryParams, &res, conn, released)
		} else if ok {
			handleGetFile(fileName, contentType, &res)
		} else {
			res.StatusCode = http.StatusNotImplemented
		}
	default:
		res.StatusCode = http.StatusNotImplemented
	}

	// apply the status text to the res object
	res.Status = http.StatusText(res.StatusCode)
	// write the http.Response object to the connection
	res.Write(conn)
	// properly close the connection once the file has been serviced
	conn.Close()
}

// main handles the entrypoint into the http_server logic flow
func main() {
	var port int

	if len(os.Args) > 1 {
		var err error
		port, err = strconv.Atoi(os.Args[1])
		if err != nil {
			log.Fatal("usage: http-server <port>")
		}
	} else {
		log.Fatal("usage: http-server <port>")
	}

	// create a folder to segment the uploaded files
	if _, err := os.Stat(outFolder); os.IsNotExist(err) {
		err := os.Mkdir(outFolder, 0755)
		if err != nil {
			log.Fatalf("Failed to create directory: %s", err)
		}
	}

	// Start the listener on the specified port
	fmt.Println("Starting listener on port", port)
	portString := fmt.Sprintf(":%d", port)
	ln, err := net.Listen("tcp", portString)
	if err != nil {
		log.Printf("Failed to start listener: %s", err)
		return
	}

	// handle incoming connections and spawn off additional goroutines
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %s", err)
			continue
		}

		// block until there are less than maxConCurrent threads running
		sem.Acquire()

		// create a goroutine to handle this connection
		go func(conn net.Conn) {
			// allow the handleConn function to preemptively release the
			// semaphore in the case of the long running clients
			released := false

			// release the semaphore on connection termination
			defer func() {
				if !released {
					sem.Release()
				}
			}()

			// actually process this request
			handleConn(conn, &released)
		}(conn)
	}
}
