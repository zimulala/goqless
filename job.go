package goqless

import (
	"github.com/garyburd/redigo/redis"
	"reflect"
	"strings"
	"time"
	// "encoding/json"
)

var (
	JOBSTATES   = []string{"stalled", "running", "scheduled", "depends", "recurring"}
	finishBytes = []byte(`{"finish":"yes"}`)
)

type History struct {
	When   int64
	Q      string
	What   string
	Worker string
}

type Job struct {
	Jid          string
	Klass        string
	State        string
	Queue        string
	Worker       string
	Tracked      bool
	Priority     int
	Expires      int64
	Retries      int
	Remaining    int
	Data         interface{}
	Tags         StringSlice
	History      []History
	Failure      interface{}
	Dependents   StringSlice
	Dependencies interface{}

	cli *Client
}

func NewJob(cli *Client) *Job {
	return &Job{
		Expires:      time.Now().Add(time.Hour * 60).UTC().Unix(), // hour from now
		Dependents:   nil,                                         // []interface{}{},
		Tracked:      false,
		Tags:         nil,
		Jid:          generateJID(),
		Retries:      5,
		Data:         nil,
		Queue:        "mock_queue",
		State:        "running",
		Remaining:    5,
		Failure:      nil,
		History:      nil, // []interface{}{},
		Dependencies: nil,
		Klass:        "Job",
		Priority:     0,
		Worker:       "mock_worker",
		cli:          cli,
	}
}

func (j *Job) Client() *Client {
	return j.cli
}

func (j *Job) SetClient(cli *Client) {
	j.cli = cli
}

// Move this from it's current queue into another
func (j *Job) Move(queueName string) (string, error) {
	now := timestamp()
	return redis.String(j.cli.Do("qless", 0, "put", now, queueName, j.Jid, j.Klass, marshal(j.Data), now, 0))
}

// Fail(0, id, worker, type, message, now, [data])
// Fail this job
// return success, error
func (j *Job) Fail(typ, message string) (bool, error) {
	now := timestamp()
	return Bool(j.cli.Do("qless", 0, "fail", now, j.Jid, j.Worker, typ, message, now, marshal(j.Data)))
}

// Heartbeat(0, id, worker, now, [data])
// Heartbeats this job
// return success, error
func (j *Job) Heartbeat() (bool, error) {
	now := timestamp()
	return Bool(j.cli.Do("qless", 0, "heartbeat", now, j.Jid, j.Worker, now, marshal(j.Data)))
}

// Complete(0, jid, worker, queue, now, data, ['next', n, [('delay', d) | ('depends', '["jid1","jid2",...]')])
// Completes this job
// returns state, error
func (j *Job) Complete() (string, error) {
	now := timestamp()
	return redis.String(j.cli.Do("qless", 0, "complete", now, j.Jid, j.Worker, j.Queue, now, marshal(j.Data)))
}

//for big job, save memory in redis
func (j *Job) CompleteWithNoData() (string, error) {
	now := timestamp()
	return redis.String(j.cli.Do("qless", 0, "complete", now, j.Jid, j.Worker, j.Queue, now, finishBytes))
}

func (j *Job) HeartbeatWithNoData() (bool, error) {
	now := timestamp()
	return Bool(j.cli.Do("qless", 0, "heartbeat", now, j.Jid, j.Worker, now))
}

// Cancel(0, id)
// Cancels this job
func (j *Job) Cancel() {
	j.cli.Do("qless", 0, "cancel", timestamp(), j.Jid)
}

// Track(0, 'track', jid, now, tag, ...)
// Track this job
func (j *Job) Track() (bool, error) {
	now := timestamp()
	return Bool(j.cli.Do("qless", 0, "track", now, "track", j.Jid, now, ""))
}

// Track(0, 'untrack', jid, now)
// Untrack this job
func (j *Job) Untrack() (bool, error) {
	now := timestamp()
	return Bool(j.cli.Do("qless", 0, "track", now, "untrack", j.Jid, now))
}

// Tag(0, 'add', jid, now, tag, [tag, ...])
func (j *Job) Tag(tags ...interface{}) (string, error) {
	now := timestamp()
	args := []interface{}{0, "tag", now, "add", j.Jid, now}
	args = append(args, tags...)
	return redis.String(j.cli.Do("qless", args...))
}

// Tag(0, 'remove', jid, now, tag, [tag, ...])
func (j *Job) Untag(tags ...interface{}) (string, error) {
	now := timestamp()
	args := []interface{}{0, "tag", now, "remove", j.Jid, now}
	args = append(args, tags...)
	return redis.String(j.cli.Do("qless", args...))
}

// Retry(0, jid, queue, worker, now, [delay])
func (j *Job) Retry(delay int) (int, error) {
	return redis.Int(j.cli.Do("qless", 0, "retry", timestamp(), j.Jid, j.Queue, j.Worker, delay))
}

// Depends(0, jid, 'on', [jid, [jid, [...]]])
func (j *Job) Depend(jids ...interface{}) (string, error) {
	args := []interface{}{0, "depends", timestamp(), j.Jid, "on"}
	args = append(args, jids...)
	return redis.String(j.cli.Do("qless", args...))
}

// Depends(0, jid, 'off', ('all' | [jid, [jid, [...]]]))
func (j *Job) Undepend(jids ...interface{}) (string, error) {
	args := []interface{}{0, "depends", timestamp(), j.Jid, "off"}
	args = append(args, jids...)
	return redis.String(j.cli.Do("qless", args...))
}

type RecurringJob struct {
	Tags     StringSlice
	Jid      string
	Retries  int
	Data     interface{}
	Queue    string
	Interval int
	Count    int
	Klass    string
	Priority int

	cli *Client
}

func NewRecurringJob(cli *Client) *RecurringJob {
	return &RecurringJob{cli: cli}
}

// example: job.Update(map[string]interface{}{"priority": 5})
// options:
//   priority int
//   retries int
//   interval int
//   data interface{}
//   klass string
func (r *RecurringJob) Update(opts map[string]interface{}) {
	args := []interface{}{0, "recur", timestamp(), "update", r.Jid}

	vOf := reflect.ValueOf(r).Elem()
	for key, value := range opts {
		key = strings.ToLower(key)
		v := vOf.FieldByName(ucfirst(key))
		if v.IsValid() {
			setv := reflect.ValueOf(value)
			if key == "data" {
				setv = reflect.ValueOf(marshal(value))
			}
			v.Set(setv)
			args = append(args, key, value)
		}
	}

	r.cli.Do("qless", args...)
}

func (r *RecurringJob) Cancel() {
	r.cli.Do("qless", 0, "recur", timestamp(), "off", r.Jid)
}

func (r *RecurringJob) Tag(tags ...interface{}) {
	args := []interface{}{0, "recur", timestamp(), "tag", r.Jid}
	args = append(args, tags...)
	r.cli.Do("qless", args...)
}

func (r *RecurringJob) Untag(tags ...interface{}) {
	args := []interface{}{0, "recur", timestamp(), "untag", r.Jid}
	args = append(args, tags...)
	r.cli.Do("qless", args...)
}
