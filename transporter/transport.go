/* The transporter layer */

package transporter

type Message struct {
	Destination string
	Source      string
	Command     []byte
}

type Transporter struct {
	Send chan<- *Message
	Recv <-chan *Message
}
