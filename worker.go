// This worker does not model the way qless does it.
// I more or less modeled it after my own needs.
package goqless

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

type JobFunc func(*Job) error
type JobCallback func(*Job) error

type Worker struct {
	sync.Mutex
	Interval int // in time.Duration

	funcs     map[string]JobFunc
	queue     *Queue
	queueAddr string
	queueName string

	cli *Client
}

func NewWorker(queueAddr string, queueName string, interval int) (*Worker, error) {
	ipport := strings.Split(queueAddr, ":")
	client, err := Dial(ipport[0], ipport[1])
	if err != nil {
		log.Println("Dial err:", err)
		return nil, errors.New(err.Error())
	}

	w := &Worker{
		Interval: interval,
		funcs:    make(map[string]JobFunc),
		// events: c.Events(),
		cli:       client,
		queueName: queueName,
		queueAddr: queueAddr,
	}

	w.queue = w.cli.Queue(queueName)

	return w, nil
}

func heartbeatStart(job *Job, done chan bool, heartbeat int, l sync.Locker) {
	tick := time.NewTicker(time.Duration(heartbeat) * time.Duration(time.Second))
	for {
		select {
		case <-done:
			tick.Stop()
			return
		case <-tick.C:
			l.Lock()
			success, err := job.HeartbeatWithNoData()
			l.Unlock()
			if err != nil {
				log.Printf("failed HeartbeatWithNoData jid:%v, queue:%v, success:%v, error:%v",
					job.Jid, job.Queue, success, err)
			} else {
				log.Printf("warning, slow, HeartbeatWithNoData jid:%v, queue:%v, success:%v",
					job.Jid, job.Queue, success)
			}
		}
	}
}

//try our best to complete the job
func (w *Worker) tryCompleteJob(job *Job) error {
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second)
		log.Println("tryCompleteJob", job.Jid)
		ipport := strings.Split(w.queueAddr, ":")
		client, err := Dial(ipport[0], ipport[1])
		if err != nil {
			log.Println("Dial err:", err)
			continue
		}

		job.SetClient(client)
		if _, err := job.CompleteWithNoData(); err != nil {
			client.Close()
			log.Println("tryCompleteJob", job.Jid, err)
		} else {
			client.Close()
			return nil
		}
	}

	return errors.New("tryCompleteJob " + job.Jid + "failed")
}

func (w *Worker) Start() error {
	// log.Println("worker Start")

	defer func() {
		w.cli.Close()
		w.cli = nil
	}()

	heartbeatStr, err := w.cli.GetConfig("heartbeat")
	heartbeat, err := strconv.Atoi(heartbeatStr)
	log.Println("heartbeatStr:", heartbeat)
	if err != nil {
		heartbeat = 60
		log.Println(err)
	}

	heartbeat /= 2

	err = func(q *Queue) error {
		for {
			w.Lock()
			jobs, err := q.Pop(1) //we may pop more if fast enough
			w.Unlock()

			if err != nil {
				log.Println(err)
				return err
			}

			if len(jobs) == 0 {
				time.Sleep(time.Duration(w.Interval) * time.Millisecond)
				continue
			}

			for i := 0; i < len(jobs); i++ {
				if len(jobs[i].History) > 2 {
					log.Printf("warning, multiple processed exist %+v\n", jobs[i])
				}
				done := make(chan bool)
				//todo: using seprate connection to send heartbeat
				go heartbeatStart(jobs[i], done, heartbeat, w)
				f, ok := w.funcs[jobs[i].Klass]
				if !ok { //we got a job that not belongs to us
					done <- false
					log.Fatalf("got a message not belongs to us, queue %v, job %+v\n", q.Name, jobs[i])
					continue
				}

				err := f(jobs[i])
				if err != nil {
					// TODO: probably do something with this
					log.Println("error: job failed, jid", jobs[i].Jid, "queue", jobs[i].Queue, err.Error())
					w.Lock()
					success, err := jobs[i].Fail("fail", err.Error())
					w.Unlock()
					done <- false
					if err != nil {
						log.Printf("fail job:%+v success:%v, error:%v",
							jobs[i].Jid, success, err)
						return err
					}
				} else {
					w.Lock()
					status, err := jobs[i].CompleteWithNoData()
					w.Unlock()
					done <- true
					if err != nil {
						err = w.tryCompleteJob(jobs[i])
						if err != nil {
							log.Printf("fail job:%+v status:%v, error:%v",
								jobs[i].Jid, status, err)
						} else {
							log.Println("retry complete job ", jobs[i].Jid, "ok")
						}
						return errors.New("restart")
					} else {
						if status != "complete" {
							log.Printf("job:%+v status:%v", jobs[i].Jid, status)
						}
					}
				}
			}
		}
		return nil
	}(w.queue)

	return err
}

func (w *Worker) AddFunc(name string, f JobFunc) error {
	if _, ok := w.funcs[name]; ok {
		return fmt.Errorf("function \"%s\" already exists", name)
	}

	w.funcs[name] = f
	return nil
}

// Adds all the methods in the passed interface as job functions.
// Job names are in the form of: name.methodname
func (w *Worker) AddService(name string, rcvr interface{}) error {
	typ := reflect.TypeOf(rcvr)
	val := reflect.ValueOf(rcvr)
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		w.AddFunc(name+"."+method.Name, func(job *Job) error {
			ret := method.Func.Call([]reflect.Value{val, reflect.ValueOf(job)})
			if len(ret) > 0 {
				if err, ok := ret[0].Interface().(error); ok {
					return err
				}
			} else {
				errStr := "reflect len less than zero." + strconv.Itoa(len(ret))
				return errors.New(errStr)
			}

			return nil
		})
	}

	return nil
}
