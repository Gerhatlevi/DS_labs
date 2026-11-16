package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

// mutex to protect writes to the global topic data
var topicMu sync.Mutex

// subChannelSize is used for tracking the size of the channels in the
// subChannelMap mapping. This allows for a max of subChannelSize messages
// to be placed into each queue.
const subChannelSize = 10

// eventsTimeoutSec is used for setting the timeout on a reading client
// that is waiting for an event on a provided topic.
const eventsTimeoutSec = 15

// Subscriber is a struct to compactly manage subscriber ids and their
// corresponding Buffered Channel for messages
type Subscriber struct {
	id     string
	buffer chan string
	lock   *sync.Mutex
}

// topic map to keep track of topics and subscribers for event publishing
// NOTE: this could probably be better implemented with a different more
// performant data structure for looking up ids to be more scalable for
// a large N number of subscribers per topic.
var topicMap = make(map[string][]*Subscriber)

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
	subscriber := Subscriber{id: uuid.String(), buffer: make(chan string, subChannelSize), lock: &sync.Mutex{}}
	topicMap[topic] = append(topicMap[topic], &subscriber)
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
	success := false
	res.StatusCode = http.StatusOK

	topicMu.Lock()
	// need to check if id in the topicMap and if so, remove from the list
	for idx, subscriber := range topicMap[topic] {
		if subscriber.id == id {
			// delete the element from the slice
			log.Printf("Removing UUID %s from topic %s", id, topic)
			topicMap[topic] = slices.Delete(topicMap[topic], idx, idx+1)
			success = true
			break
		}
	}
	topicMu.Unlock()

	if !success {
		res.StatusCode = http.StatusNotFound
	}

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

		for _, subscriber := range idList {
			// write the content of the message to each subscriber buffer
			subscriber.lock.Lock()
			channelLen := len(subscriber.buffer)

			// only write to this channel if there is space in the buffer
			if channelLen < subChannelSize {
				subscriber.buffer <- string(message)
			}
			subscriber.lock.Unlock()
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
func handleEvents(topic string, id string, res *http.Response, conn net.Conn, released *bool) {
	if idList, ok := topicMap[topic]; ok {
		res.StatusCode = http.StatusOK

		// ensure the provided ID is subscribed to the topic
		subscriber := func(uuid string, list []*Subscriber) *Subscriber {
			for _, sub := range list {
				if sub.id == uuid {
					return sub
				}
			}
			return nil
		}(id, idList)

		if subscriber == nil {
			res.StatusCode = http.StatusNotFound
			res.Body = bodyWrapper(fmt.Sprintf("{\"error\": \"ID: %s is not subscribed to topic: %s\"}\n", id, topic))
			return
		}

		// release the semaphore to allow other tasks to process
		*released = true
		sem.Release()

		// set the timeout for this connection
		timeout := eventsTimeoutSec * time.Second
		startTime := time.Now()

		// sit forever and let the publisher do it's work
		for {
			// check this channel to see if there is data available to write
			subscriber.lock.Lock()
			channelLen := len(subscriber.buffer)

			// only read from this Channel if there is data to be consumed
			// otherwise the reader is blocked until data comes in on the
			// Channel. This will consume all current messages in the queue.
			messages := []string{}
			for range channelLen {
				msg := <-subscriber.buffer
				messages = append(messages, msg)
			}
			subscriber.lock.Unlock()

			// write the data and close this connection
			if len(messages) > 0 {
				messagesJson, err := json.Marshal(messages)
				if err != nil {
					log.Printf("Failed to marshal messages to json: %s", err)
					messagesJson, _ = json.Marshal("error")
				}
				res.Body = bodyWrapper(string(messagesJson))
				res.Write(conn)
				conn.Close()
				return
			}

			// check the timeout
			if time.Since(startTime) > timeout {
				res.StatusCode = http.StatusNoContent
				res.Write(conn)
				conn.Close()
				return
			}
		}
	} else {
		res.StatusCode = http.StatusBadRequest
		res.Body = bodyWrapper(fmt.Sprintf("{\"error\": \"topic: %s does not exist\"}\n", topic))
	}
}

// HandleSubscribe is the main wrapper handler that invokes handleSubscribe
// which manages creating and adding a particular id to the desired topic.
func HandleSubscribe(queryParams url.Values, res *http.Response) {
	// default to a bad request and have other functions set the proper
	// status codes on different code flow
	res.StatusCode = http.StatusBadRequest
	if topicList := queryParams["topic"]; topicList != nil {
		// only one topic is allowed to be subscribed to at a time
		if len(topicList) == 1 {
			handleSubscribe(topicList[0], res)
		}
	}
}

// HandleUnsubscribe is the main wrapper handler that invokes handleUnsubscribe
// which manages removing a particular id from a topic.
func HandleUnsubscribe(queryParams url.Values, res *http.Response) {
	// default to a bad request and have other functions set the proper
	// status codes on different code flow
	res.StatusCode = http.StatusBadRequest

	// there must be an id associates with this unsubscribe request
	if idList := queryParams["id"]; idList != nil {
		// only one id is allowed in this request
		if len(idList) != 1 {
			res.StatusCode = http.StatusBadRequest
			return
		}

		// topic must also be provided in this request
		if topicList := queryParams["topic"]; topicList != nil {
			// only one topic is allowed in this request
			if len(topicList) == 1 {
				handleUnsubscribe(topicList[0], idList[0], res)
			}
		}
	}
}

// HandlePublish is the main handler wrapper that invokes handlePublish to perform the actual
// work for this client connection to write data for a particular topic.
func HandlePublish(queryParams url.Values, req *http.Request, res *http.Response) {
	// default to a bad request and have other functions set the proper
	// status codes on different code flow
	res.StatusCode = http.StatusBadRequest

	if topicList := queryParams["topic"]; topicList != nil {
		// only one topic is allowed to be subscribed to at a time
		if len(topicList) == 1 {
			handlePublish(topicList[0], req, res)
		}
	}
}

// HandleEvents is the main handler wrapper that invokes handleEvents to perform the actual work
// of managing this connection and writing any messages that were published on the provided topic.
func HandleEvents(queryParams url.Values, res *http.Response, conn net.Conn, released *bool) {
	// default to a bad request and have other functions set the proper
	// status codes on different code flow
	res.StatusCode = http.StatusBadRequest

	// there must be an id associates with this events request
	if idList := queryParams["id"]; idList != nil {
		// only one id is allowed in this request
		if len(idList) != 1 {
			res.StatusCode = http.StatusBadRequest
			return
		}
		if topicList := queryParams["topic"]; topicList != nil {
			// only one topic is allowed in this request
			if len(topicList) == 1 {
				handleEvents(topicList[0], idList[0], res, conn, released)
			}
		}
	}
}
