package mq

import (
	//"strconv"
	"github.com/bitly/go-nsq"
	//"encoding/binary"
	//"fmt"
)

type Nsq struct {
	pub       *nsq.Producer
	msgCount  int
	msgSize   int
	topic     string
	topic_raw string
}

func NewNsq(msgCount int, msgSize int, topic_raw string) *Nsq {
	//topic := "0#ephemeral"
	topic := topic_raw
	topic += "m#ephemeral"

	pub, _ := nsq.NewProducer("localhost:4150", nsq.NewConfig())
	//	if i >= 128 {
	//		pub, _ = nsq.NewProducer("192.168.1.11:4150", config)
	//	}

	return &Nsq{
		pub:       pub,
		msgCount:  msgCount,
		msgSize:   msgSize,
		topic:     topic,
		topic_raw: topic_raw,
	}
}

func (n *Nsq) Teardown() {
	n.pub.Stop()
}

func (n *Nsq) Send(message []byte) {
	//message=append(message, "\n"...)
	//ch := make([]byte, 3)
	//binary.PutVarint(ch, strconv.Atoi(n.channel))
	//message=append(message, ch)
	message = append(message, n.topic_raw...)
	message = append(message, "\n"...)
	b := make([]byte, n.msgSize-len(message))
	//fmt.Printf("~~~~~~~~~~~~~~%d\n", len(message))
	//fmt.Println(message)
	message = append(message, b...)
	//fmt.Printf("%d~~~~~~~~~~\n", len(message))
	n.pub.PublishAsync(n.topic, message, nil)
}
