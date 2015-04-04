An implementation of the Raft distributed consensus algorithm in Go.

Uses a Transport struct to abstract the details of network communication. The Raft logic operates by passing messages to other nodes through channels to the Transport layer and by passing committed state-machine commands for whatever underlying storage mechanism through another channel. This approach allows emulation of network unreliability by putting an unreliable router at the end of the Transport channels.
