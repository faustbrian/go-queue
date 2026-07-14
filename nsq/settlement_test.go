package nsq

import (
	"testing"
	"time"

	"github.com/faustbrian/go-queue/job"
	nsqgo "github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/require"
)

type messageDelegate struct {
	finished int
	requeued int
}

func (d *messageDelegate) OnFinish(*nsqgo.Message)                       { d.finished++ }
func (d *messageDelegate) OnRequeue(*nsqgo.Message, time.Duration, bool) { d.requeued++ }
func (d *messageDelegate) OnTouch(*nsqgo.Message)                        {}

func TestRequestDefersNSQSettlement(t *testing.T) {
	delegate := &messageDelegate{}
	queued := job.NewTask(nil)
	message := nsqgo.NewMessage(nsqgo.MessageID{}, queued.Bytes())
	message.Delegate = delegate
	tasks := make(chan *nsqgo.Message, 1)
	tasks <- message
	worker := &Worker{opts: newOptions(), tasks: tasks}
	worker.startOnce.Do(func() {})

	task, err := worker.Request()
	require.NoError(t, err)
	require.Zero(t, delegate.finished)
	require.Zero(t, delegate.requeued)

	delivery := task.(*job.Message)
	require.NoError(t, delivery.Ack())
	require.Equal(t, 1, delegate.finished)

	requeueDelegate := &messageDelegate{}
	requeueMessage := nsqgo.NewMessage(nsqgo.MessageID{}, queued.Bytes())
	requeueMessage.Delegate = requeueDelegate
	requeueTasks := make(chan *nsqgo.Message, 1)
	requeueTasks <- requeueMessage
	requeueWorker := &Worker{opts: newOptions(), tasks: requeueTasks}
	requeueWorker.startOnce.Do(func() {})
	requeueTask, err := requeueWorker.Request()
	require.NoError(t, err)

	require.NoError(t, requeueTask.(*job.Message).Nack())
	require.Equal(t, 1, requeueDelegate.requeued)
}
