package mq

import (
	//"strconv"
	"github.com/bitly/go-nsq"
	"github.com/Chong-Li/RTM-test/publisher/benchmark"
	//"encoding/binary"
	//"fmt"
)

type Nsq struct {
	handler benchmark.MessageHandler
	pub     *nsq.Producer
	sub     *nsq.Consumer
	topic   string
	channel string
}

func NewNsq(numberOfMessages int, testLatency bool, channeL string) *Nsq {
	//topic := "0#ephemeral"
	channel := channeL
	channel += "m#ephemeral"
	topic := channel
	
        pub, _ := nsq.NewProducer("localhost:4150", nsq.NewConfig())
//	if i >= 128 {
//		pub, _ = nsq.NewProducer("192.168.1.11:4150", config)
//	}

	config := nsq.NewConfig()
	config.MaxInFlight = 1000
	config.OutputBufferSize=-1
	sub, _ := nsq.NewConsumer("111xxx", "xxx", config)

	var handler benchmark.MessageHandler
	if testLatency {
		handler = &benchmark.LatencyMessageHandler{
			NumberOfMessages: numberOfMessages,
			Latencies:        []float32{},
			Channel:	channeL,
		}
	} else {
		handler = &benchmark.ThroughputMessageHandler{NumberOfMessages: numberOfMessages}
	}

	return &Nsq{
		handler: handler,
		pub:     pub,
		sub:     sub,
		topic:   topic,
		channel: channeL,
	}
}

func (n *Nsq) Setup() {
	/*n.sub.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		n.handler.ReceiveMessage(message.Body)
		return nil
	}))
	n.sub.ConnectToNSQD("localhost:4150")*/
}

func (n *Nsq) Teardown() {
	n.sub.Stop()
	n.pub.Stop()
}

func (n *Nsq) Send(message []byte) {
	//message=append(message, "\n"...)
	//ch := make([]byte, 3)
	//binary.PutVarint(ch, strconv.Atoi(n.channel))
	//message=append(message, ch)
	message=append(message, n.channel...)
	message=append(message, "\n"...)
	b:=make([]byte, 512-len(message))
	//fmt.Printf("~~~~~~~~~~~~~~%d\n", len(message))
	//fmt.Println(message)
	message=append(message, b...)
	//fmt.Printf("%d~~~~~~~~~~\n", len(message))
	n.pub.PublishAsync(n.topic, message, nil)
}

func (n *Nsq) MessageHandler() *benchmark.MessageHandler {
	return &n.handler
}
