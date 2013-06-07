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

func haertbeatStart(job *Job, done chan bool, heartbeat int, clientLock sync.Mutex) {

	tick := time.Tick(time.Duration(heartbeat-5) * time.Duration(time.Second))
	for {
		select {
		case <-done:
			return
		case <-tick:
			clientLock.Lock()
			job.Heartbeat()
			clientLock.Unlock()
			log.Printf("heartbeat***%v,cli:%+v", job.Jid, job.cli)
		}
	}
}

func (w *Worker) Start() error {
	// log.Println("worker Start")
	var clientLock sync.Mutex

	err := func(q *Queue) error {
		heartbeatStr, err := w.cli.GetConfig("heartbeat")
		heartbeat, err := strconv.Atoi(heartbeatStr)
		//log.Println("heartbeatStr:", heartbeat)
		if err != nil {
			heartbeat = 60
		}
		for {
			clientLock.Lock()
			jobs, err := q.Pop(1)
			clientLock.Unlock()

			if err != nil {
				return err
			} else {
				if len(jobs) > 0 {
					done := make(chan bool)
					go haertbeatStart(jobs[0], done, heartbeat, clientLock)

					err := w.funcs[jobs[0].Klass](jobs[0])
					if err != nil {
						// TODO: probably do something with this
						clientLock.Lock()
						jobs[0].Fail("fail", err.Error())
						clientLock.Unlock()
						done <- false
					} else {
						clientLock.Lock()
						jobs[0].Complete()
						clientLock.Unlock()
						done <- true
						//log.Printf("===job:%+v", jobs[0])
					}
				} else {
					time.Sleep(time.Duration(w.Interval))
				}
			}
		}
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
