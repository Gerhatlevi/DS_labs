package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	maxConCurrent = 10
)

type LockManager struct {
	mu    sync.Mutex
	locks map[string]*sync.RWMutex
}

func NewLockManager() *LockManager {
	return &LockManager{locks: make(map[string]*sync.RWMutex)}
}

func (m *LockManager) ForFile(path string) *sync.RWMutex {
	m.mu.Lock()
	defer m.mu.Unlock()

	if lock := m.locks[path]; lock != nil {
		return lock
	}

	lock := &sync.RWMutex{}
	m.locks[path] = lock
	return lock
}

var locks = NewLockManager()

// https://go-cookbook.com/snippets/concurrency/semaphores
type Semaphore struct {
	Channel chan struct{}
}

func (s *Semaphore) Acquire() {
	s.Channel <- struct{}{}
}
func (s *Semaphore) Release() {
	<-s.Channel
}

var rwMutex = &sync.RWMutex{}

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
	res.Header.Set(http.CanonicalHeaderKey(s), v)
}

// readFile takes care of reading a file and passing back the read buffer
// to stream back to the user for a GET request
func readFile(f string) (io.Reader, error) {
	file, err := os.Open(f)
	if err != nil {
		return nil, err
	}

	// create the read buffer to stream to the user
	r := bufio.NewReader(file)
	return r, nil
}

// writeFile takes care of writing a stream that came from the user POST
// request and writing to the desired file name. Heavily inspired from
// https://stackoverflow.com/questions/1821811/how-to-read-write-from-to-a-file-using-go
func writeFile(name string, f multipart.File) error {
	r := bufio.NewReader(f)

	// create the output file if not exists
	out, err := os.Create(name)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(out)

	// copy over chunks of 1024 bytes from the input multipart file
	// to the newly created file
	buf := make([]byte, 1024)
	for {
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
	}
	if err = w.Flush(); err != nil {
		return err
	}
	return nil
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
	// split on / so that we don't allow directory traversal
	// we will only take the last value if multiple / are used in the uri
	fileList := strings.Split(uri, "/")
	fileName := fileList[len(fileList)-1]
	extList := strings.Split(fileName, ".")
	ext := extList[len(extList)-1]
	contentType, ok := extensionToContentType()[ext]

	// only handle this request if it is a supported file extension
	if ok {
		lock := locks.ForFile(fileName)
		switch req.Method {
		case "POST":
			// Collect the file that was sent in this request and write to disk
			file, _, err := req.FormFile("file")
			if err != nil {
				res.StatusCode = http.StatusBadRequest
				break
			}
			defer file.Close()
			lock.Lock()
			err = writeFile(fileName, file)
			lock.Unlock()
			if err != nil {
				res.StatusCode = http.StatusBadRequest
				res.Body = bodyWrapper(fmt.Sprintf("", err))
				break
			}
			res.StatusCode = http.StatusCreated
		case "GET":
			lock := locks.ForFile(fileName)
			// Read the requested file from disk and serve the file
			// serve a 404 if the file cannot be found
			lock.RLock()
			fd, err := readFile(fileName)
			lock.RUnlock()
			if err != nil {
				res.StatusCode = http.StatusNotFound
				break
			} else {
				res.StatusCode = http.StatusOK
				setHeader(res, "Content-type", contentType)
				res.Body = io.NopCloser(fd)
			}
		default:
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
	var port int

	if len(os.Args) > 1 {
		var err error
		port, err = strconv.Atoi(os.Args[1])
		if err != nil {
			log.Fatal("usage: http_server <port>")
		}
	} else {
		log.Fatal("usage: http_server <port>")
	}

	// Start the listener on the specified port
	fmt.Println("Starting listener on port", port)
	portString := fmt.Sprintf(":%d", port)
	ln, err := net.Listen("tcp", portString)
	if err != nil {
		log.Printf("Failed to start listener: %s", err)
		return
	}

	sem := &Semaphore{Channel: make(chan struct{}, maxConCurrent)}
	// handle incoming connections and spawn off additional goroutines
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %s", err)
			continue
		}

		sem.Acquire()
		// create a goroutine to handle this connection
		// NOTE: need to add in a check here to ensure we aren't exceeding maxConcurrent
		go func(conn net.Conn) {
			defer sem.Release()
			handleConn(conn)
		}(conn)

	}
}
