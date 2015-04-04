package transporter

type msgChanPair struct {
	send chan *Message
	recv chan *Message
}

func MakeChannelTransporters(hostnames []string) (transports map[string]Transporter, err error) {
	hostMapping := make(map[string]msgChanPair)

	for _, hostname := range hostnames {
		sendChan := make(chan *Message, 5)
		recvChan := make(chan *Message, 5)
		chanPair := msgChanPair{sendChan, recvChan}
		hostMapping[hostname] = chanPair
	}

	for _, hostname := range hostnames {
		go runChannelTransport(hostMapping, hostname)
	}

	transports = make(map[string]Transporter)
	for hostname, chanPair := range hostMapping {
		transports[hostname] = Transporter{chanPair.send, chanPair.recv}
	}
	return
}

func runChannelTransport(hostMapping map[string]msgChanPair, hostname string) {
	clientSend := hostMapping[hostname].send

	for msg := range clientSend {
		dst := msg.Destination
		hostMapping[dst].recv <- msg
	}
}
