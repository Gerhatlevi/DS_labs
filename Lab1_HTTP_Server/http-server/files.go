package main

import (
	"bufio"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"strings"
)

// readFile takes care of reading a file and passing back the read buffer
// to stream back to the user for a GET request
func readFile(name string) (io.Reader, error) {
	inPath := path.Join(outFolder, name)
	file, err := os.Open(inPath)
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
	outPath := path.Join(outFolder, name)
	r := bufio.NewReader(f)

	// create the output file if not exists
	out, err := os.Create(outPath)
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
