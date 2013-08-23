// This worker does not model the way qless does it.
// I more or less modeled it after my own needs.
package goqless

import (
	// "encoding/json"
	"fmt"
	// "github.com/garyburd/redigo/redis"
	"errors"
	"log"
	"reflect"
	"strconv"
	"sync"
	"time"
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

type JobFunc func(*Job) error
type JobCallback func(*Job) error

type Worker struct {
	Interval int // in time.Duration

	funcs map[string]JobFunc
	queue *Queue
	// events *Events

	cli *Client
}

func NewWorker(cli *Client, queueName string, interval int) *Worker {
	w := &Worker{
		Interval: interval,
		funcs:    make(map[string]JobFunc),
		// events: c.Events(),
		cli: cli,
	}

	w.queue = cli.Queue(queueName)

	return w
}

func heartbeatStart(job *Job, done chan bool, heartbeat int, clientLock sync.Mutex) {
	tick := time.NewTicker(time.Duration(heartbeat) * time.Duration(time.Second))
	for {
		select {
		case <-done:
			tick.Stop()
			return
		case <-tick.C:
			clientLock.Lock()
			success, err := job.Heartbeat()
			clientLock.Unlock()
			if err != nil {
				log.Printf("warning, slow, send heartbeat***jid:%v, queue:%v, success:%v, error:%v",
					job.Jid, job.Queue, success, err)
			} else {
				log.Printf("warning, slow, send heartbeat***jid:%v, queue:%v, success:%v",
					job.Jid, job.Queue, success)
			}
		}
	}
}

func (w *Worker) Start() error {
	// log.Println("worker Start")
	var clientLock sync.Mutex

	heartbeatStr, err := w.cli.GetConfig("heartbeat")
	heartbeat, err := strconv.Atoi(heartbeatStr)
	//log.Println("heartbeatStr:", heartbeat)
	if err != nil {
		heartbeat = 60
		log.Println(err)
	}

	heartbeat /= 2

	err = func(q *Queue) error {
		for {
			clientLock.Lock()
			jobs, err := q.Pop(1) //we may pop more if fast enough
			clientLock.Unlock()

			if err != nil {
				log.Println(err)
				return err
			}

			if len(jobs) == 0 {
				time.Sleep(time.Duration(w.Interval) * time.Millisecond)
				continue
			}

			for i := 0; i < len(jobs); i++ {
				done := make(chan bool)
				//todo: using seprate connection to send heartbeat
				go heartbeatStart(jobs[i], done, heartbeat, clientLock)
				f, ok := w.funcs[jobs[i].Klass]
				if !ok { //we got a job that not belongs to us
					done <- false
					log.Fatalln("got a message not belongs to us, queue", q.Name, jobs[i])
					continue
				}

				err := f(jobs[i])
				if err != nil {
					// TODO: probably do something with this
					log.Println("error: job failed, id", jobs[i].Jid, "queue", jobs[i].Queue, err.Error())
					clientLock.Lock()
					success, err := jobs[i].Fail("fail", err.Error())
					clientLock.Unlock()
					done <- false
					if err != nil {
						log.Printf("fail job:%+v success:%v, error:%v",
							jobs[i], success, err)
						return err
					}
				} else {
					clientLock.Lock()
					success, err := jobs[i].Complete()
					clientLock.Unlock()
					done <- true
					if err != nil {
						log.Printf("complete job:%+v success:%v, error:%v",
							jobs[i], success, err)
						return err
					}
					//log.Printf("===job:%+v", jobs[0])
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
