package main

import (
	"encoding/binary"
	//"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/Chong-Li/Messaging-Service-Test/publisher/mq"
)

func newTest(msgCount, msgSize int, topic string) {

	nsq := mq.NewNsq(msgCount, msgSize, topic)

	start := time.Now().UnixNano()
	b := make([]byte, 24)
	id := make([]byte, 5)
	//b:=[]byte{}
	//time.Sleep(5000*time.Millisecond)
	for i := 0; i < msgCount; i++ {
		if i == 1 {
			time.Sleep(5 * time.Second)
		}
		binary.PutVarint(b, time.Now().UnixNano())
		binary.PutVarint(id, int64(i))
		//b=append(b, strconv.FormatInt(int64(i), 10)...)
		copy(b[19:23], id[:])

		nsq.Send(b)
		time.Sleep(4096 * time.Microsecond)
	}

	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	log.Printf("Sent %d messages in %f ms\n", msgCount, ms)
	log.Printf("Sent %f per second\n", 1000*float32(msgCount)/ms)

	nsq.Teardown()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	num, _ := strconv.Atoi(os.Args[1])
	topic, _ := strconv.Atoi(os.Args[2])
	for i := 0; i < num; i++ {
		go newTest(13000, 512, strconv.Itoa(topic+i))
	}
	for {
		time.Sleep(20 * time.Second)
	}

}
