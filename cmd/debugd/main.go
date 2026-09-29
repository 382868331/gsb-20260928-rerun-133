package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"debugservice/internal/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	flag.Parse()

	handler := server.New().Routes()
	if socketPath, ok := strings.CutPrefix(*addr, "unix:"); ok {
		_ = os.Remove(socketPath)
		listener, err := net.Listen("unix", socketPath)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("debug protocol service listening on unix:%s", socketPath)
		log.Fatal(http.Serve(listener, handler))
	}
	log.Printf("debug protocol service listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, handler))
}
