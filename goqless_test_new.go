package goqless

import (
        //"github.com/zimulala/goqless"
	"log"
	"strconv"
	"time"
)

const (
	jobs_count      = 20
	queues_capacity = 5
	priority        = 10
	//If you don't want a job to be run right away but some time in the future, you can specify a delay
	delay          = -1
	retries        = 3
	interval       = 10
	test_move      = 1
	test_fail      = 2
	test_retry     = 3
	original_queue = "original_queue"
)

var clientCh = make(chan *goqless.Client)

type dataWorker struct {
}

func main() {
	log.SetFlags(log.Lshortfile | log.Ltime)

	client, err := goqless.Dial("127.0.0.1", "6379")
	if err != nil {
		log.Println("Dial err:", err)
	}
	defer client.Close()

	originalQueue := client.Queue(original_queue)
	err = initOriginalQueue(originalQueue)
	if err != nil {
		log.Println("initQueue failed, err:", err)
	}
	batchJobs(originalQueue)

	select {}
}

func initOriginalQueue(queue *goqless.Queue) (err error) {

	var klass string
	depends := []string{}

	for i := 0; i < jobs_count; i++ {
		if i%2 == 0 {
			klass = "klass.Add"
		} else {
			klass = "klass.Sub"
		}
		tags := []string{"e", "__call", "_haha", strconv.Itoa(i)}
		queue.Put("", klass, `"dataJson"`, delay, i, tags, retries, depends)
	}

	return
}

func batchJobs(queue *goqless.Queue) (err error) {

	queueCount := int(jobs_count / queues_capacity)
	// go goqless.ChannelDialClient("127.0.0.1", "6379", clientCh)

	for i := 0; i < queueCount; i++ {
		handlePerQueue(queue, i)
	}

	return
}

func handlePerQueue(queue *goqless.Queue, num int) {
	client, err := goqless.Dial("127.0.0.1", "6379")
	if err != nil {
		log.Println("Dial err:", err)
	}
	defer client.Close()

	queueStr := "queue_name_" + strconv.Itoa(num)
	readyQueue := client.Queue(queueStr)
	for i := 0; i < queues_capacity; i++ {
		jobs, err := queue.Pop(1)
		if err != nil {
			log.Println("pop failed, count:", len(jobs), " err:", err)
			return
		} else {
			// log.Printf("jobs :%+v", jobs[0].History)
		}
		jobPut, err := readyQueue.Put(queueStr+"lll_"+strconv.Itoa(i), jobs[0].Klass, "***dataJsondddd", delay,
			jobs[0].Priority, jobs[0].Tags, jobs[0].Retries, jobs[0].Dependents)
		if err != nil {
			log.Println("put failed, err:", err)
			continue
		}
		//testJobMoveFailRetry(i, jobs[0])
		jobs[0].Complete()
		//log.Printf("---jobPut:%+v, queue:%+v", jobs[0], jobs[0].Queue)
		log.Println("==jobPut : ", jobPut, " jobsPut.Priority:", jobs[0].Priority, "\n")
	}

	startWorkers(queueStr)
}

func testJobMoveFailRetry(QueueNo int, job *goqless.Job) {

	//Move to self.queue
	if test_move == QueueNo {
		//return jid
		moveStr, err := job.Move("queue_name_1")
		if err != nil {
			log.Println("Move failed. err:", err)
		}
		log.Printf("moveStr:%+v", moveStr)
	}
	//Fail
	// if test_fail == QueueNo {
	// 	failRet, err := job.Fail("typ", "message")
	// 	if err != nil {
	// 		log.Println("Fail failed. err:", err)
	// 	}
	// 	log.Println("failRet:", failRet)
	// }
	//Retry
	// if test_retry == QueueNo {
	// 	delay := 0
	// 	retryStr, err := job.Retry(delay)
	// 	if err != nil {
	// 		log.Println("Retry failed. err:", err)
	// 	}
	// 	log.Println("retryStr:", retryStr, " jid:", job.Jid)
	// 	log.Printf("===jobPut:%+v", job)
	// }
}

func startWorkers(queueStr string) (err error) {
	log.Println("worker queues", queueStr)

	startWorker := func(queueStr string) {
		client, err := goqless.Dial("127.0.0.1", "6379")
		if err != nil {
			log.Println("Dial err:", err)
		}
		defer client.Close()

		worker_x, err := initWorker(client, queueStr)
		if err != nil {
			log.Println("initWorker failed, err:", err)
		}
		log.Println("qStr:", queueStr)
		// client.SetConfig("heartbeat", 120)
		// heartbeatStr2 := client.GetConfig("heartbeat")
		// log.Println("heartbeatStr:", heartbeatStr2)
		worker_x.Start()
	}

	go startWorker(queueStr)
	go startWorker(queueStr)

	time.Sleep(10 * time.Second)

	return
}

func initWorker(cli *goqless.Client, queue string) (worker *goqless.Worker, err error) {

	worker = goqless.NewWorker(cli, queue, interval)
	log.Printf("worker: %+v, p:%p", worker, worker)
	dataW := &dataWorker{}
	worker.AddService("klass", dataW)

	return
}

func (dataW *dataWorker) Add(job *goqless.Job) (err error) {
	job.Data = map[string]interface{}{"id": "Add"}
	time.Sleep(190 * time.Second)
	job.Data = map[string]interface{}{"id": "sleepEndAdd"}

	return
}

func (dataW *dataWorker) Sub(job *goqless.Job) (err error) {
	job.Data = map[string]interface{}{"id": "Sub"}
	return
}
