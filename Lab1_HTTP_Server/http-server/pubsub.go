package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// mutex to protect writes to the global topic data
var topicMu sync.Mutex

// mutex to protect writes to the global conn data
var connMu sync.Mutex

// topic map to keep track of topics and subscribers for event publishing
// NOTE: this could probably be better implemented with a different more
// performant data structure for looking up ids to be more scalable for
// a large N number of subscribers per topic.
var topicMap = make(map[string][]string)

// connMap is used to keep track of open connections which will receive
// messages that are published for a particular topic
var connMap = make(map[string]net.Conn)

// handleSubscribe handles the main operation of creating a new id and subscribing
// a requesting client to a particular content. This primarily creates a new entry
// in the slice in the topicMap object to keep track of subscription relations.
// UUIDs are used here instead of easily predictable ids to prevent smart users from
// unsubscribing other users trivially.
func handleSubscribe(topic string, res *http.Response) {
	// create a new subscriber uuid to bind to this topic and return to user
	uuid := uuid.New()
	topicMu.Lock()
	log.Printf("Adding UUID %s to topic %s", uuid.String(), topic)
	topicMap[topic] = append(topicMap[topic], uuid.String())
	topicMu.Unlock()

	// return the created id back to the requesting client
	setHeader(*res, "Content-type", "application/json")
	res.StatusCode = http.StatusCreated
	res.Body = bodyWrapper(fmt.Sprintf("{\"id\": \"%s\"}\n", uuid.String()))
}

// handleUnsubscribe handles the operation of removing a particular id from
// a particular topic. This function essentially just removes the id from
// the topicMap if it exists and does nothing on error to prevent information
// disclosure.
func handleUnsubscribe(topic string, id string, res *http.Response) {
	topicMu.Lock()
	log.Printf("Removing UUID %s from topic %s", id, topic)

	// need to check if id in the topicMap and if so, remove from the list
	idIdx := slices.Index(topicMap[topic], id)
	success := false
	if idIdx == -1 {
		// error case, not found in subscriber list
		res.StatusCode = http.StatusOK
	} else {
		// delete the element from the slice
		topicMap[topic] = slices.Delete(topicMap[topic], idIdx, idIdx+1)
		res.StatusCode = http.StatusOK
		success = true
	}
	topicMu.Unlock()

	// return the created id back to the requesting client
	setHeader(*res, "Content-type", "application/json")

	// NOTE: in practice we probably don't want to denote the success because
	// a user would know if they were successful in unsubscribing a user id
	// that is not their own. But in this case, we would like this for debugging.
	res.Body = bodyWrapper(fmt.Sprintf("{\"success\": \"%t\"}\n", success))
}

// handlePublish takes in a message as the POST body and sends the message to
// all active subscribers in the topicMap for the provided topic. This function
// also checks for all currently active connections and if the UUID is active
// it sends the message to that connection. Otherwise there is no message to
// be sent. A bad request is sent if the topic does not exist.
func handlePublish(topic string, req *http.Request, res *http.Response) {
	// first check if the topic has any subscribers
	// if none, then discard this message, nothing to be done
	if idList, ok := topicMap[topic]; ok {
		res.StatusCode = http.StatusOK
		// get the POST request body for publishing
		message, err := io.ReadAll(req.Body)
		if err != nil {
			// This is likely due to a connection error so unlikely
			// we will need to return but good to have this case handled
			log.Printf("Failed reading message body: %s", err)
			res.StatusCode = http.StatusBadRequest
			return
		}

		// create a new message response to write to all subscribers
		resMessage := http.Response{}
		resMessage.Proto = req.Proto
		resMessage.ProtoMajor = req.ProtoMajor
		resMessage.ProtoMinor = req.ProtoMinor
		resMessage.StatusCode = http.StatusOK
		resMessage.Header = make(map[string][]string)
		resMessage.Body = bodyWrapper(fmt.Sprintf("%s", message))

		for _, uuid := range idList {
			// lookup the client connections and send the message to each
			connMu.Lock()
			if conn, ok := connMap[uuid]; ok {
				resMessage.Write(conn)
				conn.Close()
			}
			connMu.Unlock()
		}
	} else {
		// if no subscribers, this is okay. Scream into the void
		res.StatusCode = http.StatusOK
	}
}

// handleEvents is the receiver function for clients that are performing a
// GET request for a particular topic and providing their ID. If the ID is
// not currently subscribed, then a bad request is returned with an error
// message. Otherwise, the connection is held open until a timeout and
// messages are published to this connection if it is subscribed and a message
// is published to the provided topic.
//
// NOTE: conn is needed here to write intermitten values if many messages are
// published while the client is active.
func handleEvents(topic string, id string, res *http.Response, conn net.Conn) {
	if idList, ok := topicMap[topic]; ok {
		res.StatusCode = http.StatusOK
		// ensure the provided ID is subscribed to the topic
		idIdx := slices.Index(idList, id)
		if idIdx == -1 {
			res.StatusCode = http.StatusBadRequest
			res.Body = bodyWrapper(fmt.Sprintf("{\"error\": \"ID: %s is not subscribed to topic: %s\"}\n", id, topic))
			return
		}
		// add this connection to the current list of open connections
		connMu.Lock()
		connMap[id] = conn
		connMu.Unlock()

		// sit forever and let the publisher do it's work
		for {
			// TODO: add a timeout message here or keep connection open
		}
	} else {
		res.StatusCode = http.StatusBadRequest
		res.Body = bodyWrapper(fmt.Sprintf("{\"error\": \"topic: %s does not exist\"}\n", topic))
	}
}

// handlePubSub is the global function for handling a request in
// the publisher subscriber model flow. Returns true if handle-able
// by this flow, false if further processing should be done.
//
// NOTE: there is a function side effect of modifying the res
// value. The response value, if handle-able by this function, will
// be modified to have the body, statusCode, and additional HTTP
// values allocated. No further processing is required in the res.
func handlePubSub(req *http.Request, res *http.Response, conn net.Conn) bool {
	uri := req.URL.Path
	queryParams := req.URL.Query()

	switch req.Method {
	case "POST":
		if strings.HasPrefix(uri, "/subscribe") {
			if topicList := queryParams["topic"]; topicList != nil {
				// only one topic is allowed to be subscribed to at a time
				if len(topicList) == 1 {
					handleSubscribe(topicList[0], res)
				} else {
					res.StatusCode = http.StatusBadRequest
				}
			} else {
				res.StatusCode = http.StatusBadRequest
			}
		} else if strings.HasPrefix(uri, "/unsubscribe") {
			// there must be an id associates with this unsubscribe request
			if idList := queryParams["id"]; idList != nil {
				// only one id is allowed in this request
				if len(idList) != 1 {
					res.StatusCode = http.StatusBadRequest
					return true
				}

				// topic must also be provided in this request
				if topicList := queryParams["topic"]; topicList != nil {
					// only one topic is allowed in this request
					if len(topicList) == 1 {
						handleUnsubscribe(topicList[0], idList[0], res)
					} else {
						res.StatusCode = http.StatusBadRequest
					}
				} else {
					res.StatusCode = http.StatusBadRequest
				}
			} else {
				res.StatusCode = http.StatusBadRequest
			}
		} else if strings.HasPrefix(uri, "/publish") {
			if topicList := queryParams["topic"]; topicList != nil {
				// only one topic is allowed to be subscribed to at a time
				if len(topicList) == 1 {
					handlePublish(topicList[0], req, res)
				} else {
					res.StatusCode = http.StatusBadRequest
				}
			} else {
				res.StatusCode = http.StatusBadRequest
			}
		}
	case "GET":
		if strings.HasPrefix(uri, "/events") {
			// there must be an id associates with this events request
			if idList := queryParams["id"]; idList != nil {
				// only one id is allowed in this request
				if len(idList) != 1 {
					res.StatusCode = http.StatusBadRequest
					return true
				}
				if topicList := queryParams["topic"]; topicList != nil {
					// only one topic is allowed in this request
					if len(topicList) == 1 {
						handleEvents(topicList[0], idList[0], res, conn)
					} else {
						res.StatusCode = http.StatusBadRequest
					}
				}
			} else {
				res.StatusCode = http.StatusBadRequest
			}
		}
	default:
		res.StatusCode = http.StatusNotImplemented
	}

	if res.StatusCode == http.StatusNotImplemented || res.StatusCode == 0 {
		return false
	}
	return true
}
