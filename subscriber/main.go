package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/Chong-Li/Messaging-Service-Test/subscriber/mq"
)

func newTester(msgCount, msgSize int, channel string) {
	mq.NewNsq(msgCount, testLatency, channel)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	num, _ := strconv.Atoi(os.Args[1])
	topic, _ := strconv.Atoi(os.Args[2])
	for i := 0; i < num; i++ {
		go newTest(13000, 1024, strconv.Itoa(topic+i)) //parseArgs(usage)
	}
	//tester := newTester("nsq", true, 10000, 1024, strconv.Itoa(0)) //parseArgs(usage)
	//tester.Test()
	for {
		time.Sleep(20 * time.Second)
	}
}
