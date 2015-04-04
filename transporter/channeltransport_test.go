package transporter

import (
	"reflect"
	"testing"
)

func TestChannelTransporterBasic(t *testing.T) {
	hosts := []string{"host1", "host2", "host3"}
	transports, _ := MakeChannelTransporters(hosts)
	go func() {
		transports["host1"].Send <- &Message{"host2", "host1", []byte("1 to 2")}
		transports["host1"].Send <- &Message{"host3", "host1", []byte("1 to 3")}
	}()

	go func() {
		transports["host2"].Send <- &Message{"host1", "host2", []byte("2 to 1")}
		transports["host2"].Send <- &Message{"host3", "host2", []byte("2 to 3")}
	}()

	go func() {
		transports["host3"].Send <- &Message{"host2", "host3", []byte("3 to 2")}
		transports["host3"].Send <- &Message{"host1", "host3", []byte("3 to 1")}
	}()

	msgs_for_1 := map[string]bool{
		string((<-transports["host1"].Recv).Command): true,
		string((<-transports["host1"].Recv).Command): true,
	}
	expected_msgs_for_1 := map[string]bool{
		"2 to 1": true,
		"3 to 1": true,
	}
	if !reflect.DeepEqual(msgs_for_1, expected_msgs_for_1) {
		t.Error(msgs_for_1)
	}

	msgs_for_2 := map[string]bool{
		string((<-transports["host2"].Recv).Command): true,
		string((<-transports["host2"].Recv).Command): true,
	}
	expected_msgs_for_2 := map[string]bool{
		"1 to 2": true,
		"3 to 2": true,
	}
	if !reflect.DeepEqual(msgs_for_2, expected_msgs_for_2) {
		t.Error(msgs_for_2)
	}

	msgs_for_3 := map[string]bool{
		string((<-transports["host3"].Recv).Command): true,
		string((<-transports["host3"].Recv).Command): true,
	}
	expected_msgs_for_3 := map[string]bool{
		"2 to 3": true,
		"1 to 3": true,
	}
	if !reflect.DeepEqual(msgs_for_3, expected_msgs_for_3) {
		t.Error(msgs_for_3)
	}

}
