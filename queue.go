package goqless

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
)

var _ = fmt.Sprint("")

type Queue struct {
	Running   int
	Name      string
	Waiting   int
	Recurring int
	Depends   int
	Stalled   int
	Scheduled int

	cli *Client
}

func NewQueue(cli *Client) *Queue {
	return &Queue{cli: cli}
}

func (q *Queue) SetClient(cli *Client) {
	q.cli = cli
}

// Jobs(0, ('stalled' | 'running' | 'scheduled' | 'depends' | 'recurring'), now, queue, [offset, [count]])
func (q *Queue) Jobs(state string, start, count int) ([]string, error) {
	reply, err := redis.Values(q.cli.Do("qless", 0, "jobs", timestamp(), state, q.Name))
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, val := range reply {
		s, _ := redis.String(val, err)
		ret = append(ret, s)
	}
	return ret, err
}

// Cancel all jobs in this queue
func (q *Queue) CancelAll() {
	for _, state := range JOBSTATES {
		var jids []string
		for {
			jids, _ = q.Jobs(state, 0, 100)
			for _, jid := range jids {
				j, err := q.cli.GetRecurringJob(jid)
				if j != nil && err == nil {
					j.Cancel()
				}
			}

			if len(jids) < 100 {
				break
			}
		}
	}
}

// Pause(0, name)
func (q *Queue) Pause() {
	q.cli.Do("qless", 0, "pause", timestamp(), q.Name)
}

// Unpause(0, name)
func (q *Queue) Unpause() {
	q.cli.Do("qless", 0, "unpause", timestamp(), q.Name)
}

// Puts a job into the queue
// returns jid, error
func (q *Queue) Put(jid, klass string, data interface{}, delay, priority int, tags []string, retries int, depends []string) (string, error) {
	if jid == "" {
		jid = generateJID()
	}
	if delay == -1 {
		delay = 0
	}
	if priority == -1 {
		priority = 0
	}
	if retries == -1 {
		retries = 5
	}

	return redis.String(q.cli.Do(
		"qless", 0, "put", timestamp(), q.Name, jid, klass,
		marshal(data),
		delay, "priority", priority,
		"tags", marshal(tags), "retries",
		retries, "depends", marshal(depends)))
}

// Pops a job off the queue.
func (q *Queue) Pop(count int) ([]*Job, error) {
	if count == 0 {
		count = 1
	}

	reply, err := redis.Bytes(q.cli.Do("qless", 0, "pop", timestamp(), q.Name, workerName(), count))
	if err != nil {
		return nil, err
	}

	//"{}"
	if len(reply) == 2 {
		return nil, nil
	}

	//println(string(reply))

	var jobs []*Job
	err = json.Unmarshal(reply, &jobs)
	if err != nil {
		return nil, err
	}

	for _, v := range jobs {
		v.cli = q.cli
	}

	return jobs, nil
}

// Put a recurring job in this queue
func (q *Queue) Recur(jid, klass string, data interface{}, interval, offset, priority int, tags []string, retries int) (string, error) {
	if jid == "" {
		jid = generateJID()
	}
	if interval == -1 {
		interval = 0
	}
	if offset == -1 {
		offset = 0
	}
	if priority == -1 {
		priority = 0
	}
	if retries == -1 {
		retries = 5
	}

	return redis.String(q.cli.Do(
		"qless", 0, "recur", timestamp(), "on", q.Name, jid, klass,
		data, "interval",
		interval, offset, "priority", priority,
		"tags", marshal(tags), "retries", retries))
}
