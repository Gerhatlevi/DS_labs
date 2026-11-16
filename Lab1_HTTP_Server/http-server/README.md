## Submission Documentation
All parts of this lab were completed including the advanced pub/sub section. Each of the requirements are outlined
below as to how they were satisfied and how they were tested with the provided scripts. A Dockerfile was also provided
and published to [dockerhub](https://hub.docker.com/r/yakobh/distributed-systems-labs/tags) under the tag of `http_server`
current just as an `arm64` build.

Overall organization:
The project is split into the main components under `main.go`, `files.go`, and `pubsub.go`.
- `main.go` handles the main operations for parsing and determining which functions to call for handling the client connection.
- `files.go` handles the operations for GET and POST requests to push and pull files from the server.
- `pubsub.go` handles the operations for the publisher subscriber broker and each logic for `subscribe`, `unsubscribe`, `publish`,
  and `events`.

There is also a provided `Makefile` for simplicity and `test.sh` file that runs some basic tests to ensure the implementation is
running as expected and specified.

### 1. HTTP Server
This part was completed by only using the `net` package for handling connections and does not make use of `http.ListenAndServe`.
The `net/http` package is used for parsing http requests and for sending http responses back to the client connection as
outlined in the requirements.

#### Accepting Command Line Arguments
- The command line is simply parsed by using `os.Args` and collecting the first argument as the port number
  and the error handling is performed to ensure it is a parsable integer.
- The server is then started via `net.Listen` and error handling is again performed to ensure the port number
  was valid and that a listener socket could be established.

#### Handling Client Connections
- A new Go routine is established for each new connection as specified.
- The handling of no more than 10 connections is performed via an implementation of a counting semaphore via
  a buffered channel: see `type Semaphore struct` for the implementation.
- The buffered channel approach then automatically blocks if there are too many connections until the
  `Release()` function is called and space is free to then spin off a new Go routine.

#### Accepting GET Requests
- The request for files is parsed at `/<filename>.<ext>` in the request and a mapping of supported types is
  checked via the `extensionToContentType()` map. This also helps to convert to the output `Content-type` that
  was specified in the lab description.
- If the file extension does not exist in the map, then a `400 Bad Request` is sent back to the client.
- `501 Not Implemented` was selected for base case if a request was made to a verb that is not supported.
- If the file does not exist, a proper `404 Not Found` is supplied back to the user.

#### Accepting POST Requests
- POST requests are handled similarly to GET but data is expected in the `file` parameter of the POST body.
- The file that is being sent via the POST body is then streamed directly to a file on disk under a directory
  called `files` which gets created by the server binary on startup. This same folder is what is being accessed
  in the GET requests so the files are accessible by both parties running POST or GET.
- These shared file resources are protected with a lock that gets created per file resource to improve the performance
  and not rely on a single global lock for all file read or write operations. This is also implemented via a `RWMutex`
  so that the readers can call `RLock` or `RUnlock` so that a better reader-writer consistency can be performed.
- These locks then guarantee mutual exclusion on files being written to or read which ensures that partially written
  files cannot be returned.

### 2. Pub-sub Broker
The implementation of the pub-sub broker was extended from the main http server in part 1 to include matching on routes
to the desired paths `/subscribe`, `/unsubscribe`, `/publish`, and `/events`. These url query parameters were parsed
using the `net/http` package and the `net/url` package that parses the parameters nicely.

```go
	req, err := http.ReadRequest(reader)
	...
	queryParams := req.URL.Query()
```

For the sake of this implementation, the buffer is assumed to be `string` values, however a more general approach to
use raw `[]byte` data would be preferred if generalizing this solution in the future. For sake of simplicity, we have
decided to make this a string only pub-sub broker.

The main topic data structure is implemented as a map with a `Subscriber` struct to maintain the buffered channel per
subscriber.

```go
type Subscriber struct {
	id     string
	buffer chan string
	lock   *sync.Mutex
}

var topicMap = make(map[string][]*Subscriber)
````

The lock per subscriber is also created to ensure mutual exclusion over the buffered channel since we perform length
checks to ensure the server does not block if the size is at max capacity or the reader blocking indefinitely
if the channel is empty and no publish event occurs.

The option to use long polling was implemented at a timeout value of 15 seconds. When the client connects to the events
API, it will consume the entire buffer and be returned as a JSON array of strings for each message that existed in the
queue. There is also a max of 10 messages that can sit in the queue so the messages get dropped from the subscribers if
the client cannot consume the queue fast enough. The client connection is then closed if data exists in the queue or if a
publish event happens before the timeout and the relevant data is sent to the client.

The suggestion to not include the long polling event clients in the max concurrency limit was also taken and the semaphore
is released if the client connection is accessing the `/events` API with a well formed request.

### 3. Cloud
We have followed the instructions to setup an AWS account via the AWS Learner Lab and setup a small EC2 instance. We have
also provided a `Dockerfile` which is used to build the docker image and host it on Dockerhub. This is then simply used
by pulling it down to the EC2 instance and running the docker image in a container. We then allow all traffic so that the
demo can show connecting to the public IP address at the provided port 80 we map to via running the following docker run
invocation. The docker invocation is not run as detached so that we can easily view the server messages during the demo.

```bash
$ docker run -p 80:5555 yakobh/distributed-systems-labs:http_server
```
