package main

import (
	"flag"
	"fmt"
	"log"
	"net"
)

func main() {
	// NOTE: this should also take first arg if -port is not specified
	port := flag.Int("port", 5555, "the port number to start the listener on")

	flag.Parse()

	// Start the listener on the specified port
	fmt.Println("Starting listener on port", *port)
	portString := fmt.Sprintf(":%d", *port)
	_, err := net.Listen("tcp", portString)
	if err != nil {
		log.Printf("Failed to start listener: %s", err)
		return
	}
}
