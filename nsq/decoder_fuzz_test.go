package nsq

import (
	"testing"

	"github.com/faustbrian/go-queue/job"
	nsqgo "github.com/nsqio/go-nsq"
)

func FuzzRequestDelivery(f *testing.F) {
	valid := job.NewTask(nil)
	f.Add(valid.Bytes())
	f.Add([]byte("not-json"))

	f.Fuzz(func(t *testing.T, data []byte) {
		delegate := &messageDelegate{}
		message := nsqgo.NewMessage(nsqgo.MessageID{}, data)
		message.Delegate = delegate
		tasks := make(chan *nsqgo.Message, 1)
		tasks <- message
		worker := &Worker{tasks: tasks, opts: newOptions()}
		worker.startOnce.Do(func() {})
		_, _ = worker.Request()
	})
}
