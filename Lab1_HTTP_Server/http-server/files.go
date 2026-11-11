package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

func handleFile(req *http.Request, res *http.Response) {
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
				res.Body = bodyWrapper(fmt.Sprintf("%s", err))
			}
			res.StatusCode = http.StatusCreated
		case "GET":
			// Read the requested file from disk and serve the file
			// serve a 404 if the file cannot be found
			lock.RLock()
			fd, err := readFile(fileName)
			if err != nil {
				res.StatusCode = http.StatusNotFound
			} else {
				res.StatusCode = http.StatusOK
				setHeader(*res, "Content-type", contentType)
				res.Body = io.NopCloser(fd)
			}
			lock.RUnlock()
		default:
			res.StatusCode = http.StatusNotImplemented
		}
	} else {
		res.StatusCode = http.StatusBadRequest
		res.Status = "400 Bad Request"
	}
}
