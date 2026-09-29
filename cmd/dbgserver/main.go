// Command dbgserver serves the line-delimited JSON debug protocol.
package main

import (
	"flag"
	"log"
	"net"
	"os"
	"time"

	"dbgprotocol/internal/protocol"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8031", "listen address")
	stdio := flag.Bool("stdio", false, "serve one client over stdin/stdout instead of listening")
	flag.Parse()

	if *stdio {
		protocol.NewServer().ServeConn(&stdioConn{os.Stdin, os.Stdout})
		return
	}

	network := "tcp"
	address := *addr
	if len(address) > 0 && address[0] == '/' {
		network = "unix"
		os.Remove(address)
	}
	ln, err := net.Listen(network, address)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	log.Printf("debug protocol server listening on %s", *addr)
	if err := protocol.NewServer().Listen(ln); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// stdioConn turns stdin/stdout into one bidirectional net.Conn-like
// connection for local script clients and locked-down sandboxes.
type stdioConn struct {
	*os.File
	out *os.File
}

func (c *stdioConn) Read(b []byte) (int, error)  { return c.File.Read(b) }
func (c *stdioConn) Write(b []byte) (int, error) { return c.out.Write(b) }
func (c *stdioConn) Close() error {
	c.out.Close()
	return c.File.Close()
}
func (c *stdioConn) LocalAddr() net.Addr  { return dummyAddr("stdio") }
func (c *stdioConn) RemoteAddr() net.Addr { return dummyAddr("stdio") }
func (c *stdioConn) SetDeadline(t time.Time) error {
	if err := c.File.SetReadDeadline(t); err != nil {
		return err
	}
	return c.out.SetWriteDeadline(t)
}
func (c *stdioConn) SetReadDeadline(t time.Time) error  { return c.File.SetReadDeadline(t) }
func (c *stdioConn) SetWriteDeadline(t time.Time) error { return c.out.SetWriteDeadline(t) }

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }
