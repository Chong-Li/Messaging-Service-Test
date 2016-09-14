package mq

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	"github.com/bitly/go-nsq"
)

type MessageHandler interface {
	// Process a received message. Return true if it's the last message, otherwise
	// return false.
	ReceiveMessage([]byte) bool
}

type LatencyMessageHandler struct {
	NumberOfMessages int
	Latencies        []float32
	Results          []byte
	messageCounter   int
	Channel          string
}

// Record each message's latency. The message contains the timestamp when it was sent.
// If it's the last message, compute the average latency and print it out. Return true
// if the message is the last one, otherwise return false.
func (handler *LatencyMessageHandler) ReceiveMessage(message []byte) bool {
	now := time.Now().UnixNano()
	//then, _ := binary.Varint(message)
	var then int64
	var ch string
	for i, value := range bytes.Split(message[24:29], []byte{'\n'}) {
		if i == 0 {
			ch = string(value)
		}
	}
	then, _ = binary.Varint(message[0:18])

	if ch != "0" {
		return false
	}
	//then, _ = binary.Varint(message[0:8])
	/*if handler.messageCounter-0 == handler.NumberOfMessages{
		handler.messageCounter++
		if handler.messageCounter == 2560000 {
			sum := float32(0)
			for _, latency := range handler.Latencies {
				sum += latency

			}
			avgLatency := float32(sum) / float32(len(handler.Latencies))
			//time.Sleep(5*time.Second)
			log.Printf("Mean latency for %d messages: %f ms\n", handler.NumberOfMessages,
				avgLatency)
			if handler.Channel == "0" {
				ioutil.WriteFile((handler.Channel), handler.Results, 0777)
			}
		}
		return false
	}*/
	//if handler.messageCounter < 0{
	//handler.messageCounter++
	//return false
	//}
	//if then != 0 && ch == "100" {
	if then != 0 {
		handler.Latencies = append(handler.Latencies, (float32(now-then))/1000/1000)
		if handler.Channel == "0" {
			//log.Printf("%d \n", handler.messageCounter);

			x := strconv.FormatInt(now-then, 10)
			handler.Results = append(handler.Results, x...)
			handler.Results = append(handler.Results, "\n"...)
		}
	}
	handler.messageCounter++
	//log.Printf(strconv.Itoa(handler.messageCounter))
	/*timeRecv :=make ([]byte, 19)
	binary.PutVarint(timeRecv, now)
	copy(message[19:37], timeRecv[:])
	handler.Pub.PublishAsync("x#ephemeral", message, nil)*/
	if handler.messageCounter == 13000 {
		sum := float32(0)
		for _, latency := range handler.Latencies {
			sum += latency

		}
		avgLatency := float32(sum) / float32(len(handler.Latencies))
		//time.Sleep(5*time.Second)
		log.Printf("Mean latency for %d messages: %f ms\n", handler.NumberOfMessages,
			avgLatency)
		if handler.Channel == "0" {
			ioutil.WriteFile("ee", handler.Results, 0777)
		}

	}

	return false
}

func NewNsq(numberOfMessages int, msgSize int, channeL string) {
	//topic := "0#ephemeral"
	channel := channeL
	channel += "m#ephemeral"
	topic := channel
	config := nsq.NewConfig()
	config.MaxInFlight = 1000
	config.OutputBufferSize = -1
	sub, _ := nsq.NewConsumer(topic, channel, config)

	var handler MessageHandler
	handler = &LatencyMessageHandler{
		NumberOfMessages: numberOfMessages,
		Latencies:        []float32{},
		Channel:          channeL,
	}

	sub.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		handler.ReceiveMessage(message.Body)
		return nil
	}))

	i, _ := strconv.Atoi(channeL)

	if i < 1280 {
		sub.ConnectToNSQD("10.0.0.26:4150")
	} else {
		sub.ConnectToNSQD("localhost:4150")
	}

}
