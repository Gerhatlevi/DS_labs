package main

import (
	"log"
	"net/http"
	"strings"
)

func handleSubscribe(topic string) {
	log.Print(topic)
}

func handleUnsubscribe(topic string, id string) {
	log.Print(topic)
}

func handlePublish(topic string) {
	log.Print(topic)
}

func handleEvents(topic string, id string) {
	log.Print(topic)
}

func handlePubSub(req *http.Request, res *http.Response) bool {
	uri := req.URL.Path
	queryParams := req.URL.Query()

	switch req.Method {
	case "POST":
		if strings.HasPrefix(uri, "/subscribe") {
			if topicList := queryParams["topic"]; topicList != nil {
				// only one topic is allowed to be subscribed to at a time
				if len(topicList) == 1 {
					handleSubscribe(topicList[0])
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
				if len(idList) == 1 {
					if topicList := queryParams["topic"]; topicList != nil {
						// only one topic is allowed in this request
						if len(topicList) == 1 {
							handleUnsubscribe(topicList[0], idList[0])
						} else {
							res.StatusCode = http.StatusBadRequest
						}
					} else {
						res.StatusCode = http.StatusBadRequest
					}
				} else {
					res.StatusCode = http.StatusBadRequest
				}
			}
		} else if strings.HasPrefix(uri, "/publish") {
			if topicList := queryParams["topic"]; topicList != nil {
				// only one topic is allowed to be subscribed to at a time
				if len(topicList) == 1 {
					handlePublish(topicList[0])
				} else {
					res.StatusCode = http.StatusBadRequest
				}
			} else {
				res.StatusCode = http.StatusBadRequest
			}
		} else {
			// otherwise, we don't have a handler for this request
			res.StatusCode = http.StatusNotImplemented
		}
	case "GET":
		if strings.HasPrefix(uri, "/events") {
			// there must be an id associates with this events request
			if idList := queryParams["id"]; idList != nil {
				// only one id is allowed in this request
				if len(idList) == 1 {
					if topicList := queryParams["topic"]; topicList != nil {
						// only one topic is allowed in this request
						if len(topicList) == 1 {
							handleEvents(topicList[0], idList[0])
						} else {
							res.StatusCode = http.StatusBadRequest
						}
					}
				} else {
					res.StatusCode = http.StatusBadRequest
				}
			}
		} else {
			res.StatusCode = http.StatusNotImplemented
		}
	default:
		res.StatusCode = http.StatusNotImplemented
	}

	if res.StatusCode == http.StatusNotImplemented {
		return false
	}
	return true
}
