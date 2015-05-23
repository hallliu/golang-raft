/* The transporter layer */

package transporter

const CLIENT string = "__client__"

type Message struct {
	Destination string
	Source      string
	Command     []byte
}

type Transporter struct {
	Send chan<- *Message
	Recv <-chan *Message
}
