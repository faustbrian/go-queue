//go:build integration

package rabbitmq

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

	"github.com/faustbrian/go-queue/core"
	rabbitmqqueue "github.com/faustbrian/go-rabbitmq-queues"
)

type facadeLiveConfig struct {
	Endpoints []struct {
		Host string `json:"host"`
		Port uint16 `json:"port"`
	} `json:"endpoints"`
	VirtualHost string `json:"virtual_host"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	TLS         struct {
		ServerName string `json:"server_name"`
		RootCAFile string `json:"root_ca_file"`
	} `json:"tls"`
	Jobs struct {
		Name string `json:"name"`
	} `json:"jobs"`
}

func TestLegacyPublicConstructorOpensPublishedSuccessor(t *testing.T) {
	data, err := os.ReadFile(os.Getenv("RABBITMQ_ADAPTER_LIVE_CONFIG"))
	if err != nil {
		t.Fatalf("read live configuration: %v", err)
	}
	var live facadeLiveConfig
	if err = json.Unmarshal(data, &live); err != nil || len(live.Endpoints) != 1 || live.Jobs.Name == "" {
		t.Fatalf("decode live configuration: %v", err)
	}
	rootFile, err := os.Open(live.TLS.RootCAFile)
	if err != nil {
		t.Fatalf("open root CA: %v", err)
	}
	defer rootFile.Close()
	rootCA, err := io.ReadAll(io.LimitReader(rootFile, rabbitmqqueue.MaxTLSMaterialBytes+1))
	if err != nil || len(rootCA) == 0 || len(rootCA) > rabbitmqqueue.MaxTLSMaterialBytes {
		t.Fatalf("read bounded root CA: %v", err)
	}
	password := live.Password
	config := NativeConfig{
		Connection: rabbitmqqueue.ConnectionConfig{
			Endpoints:   []rabbitmqqueue.Endpoint{{Host: live.Endpoints[0].Host, Port: live.Endpoints[0].Port}},
			VirtualHost: live.VirtualHost,
			Credentials: rabbitmqqueue.CredentialProviderFunc(func(context.Context) (rabbitmqqueue.Credentials, error) {
				return rabbitmqqueue.Credentials{Username: live.Username, Password: []byte(password)}, nil
			}),
			TLS:         rabbitmqqueue.TLSConfig{ServerName: live.TLS.ServerName, RootCAs: [][]byte{rootCA}},
			DialTimeout: 10 * time.Second,
			Heartbeat:   10 * time.Second,
			Recovery: rabbitmqqueue.RecoveryPolicy{
				MaxAttempts: 3, InitialDelay: 100 * time.Millisecond, MaxDelay: time.Second,
			},
		},
		Producer: rabbitmqqueue.ProducerConfig{Limits: rabbitmqqueue.DefaultLimits(), MaxOutstanding: 1, PublishTimeout: 10 * time.Second},
		Consumer: rabbitmqqueue.ConsumerConfig{
			Limits: rabbitmqqueue.DefaultLimits(),
			Queue:  rabbitmqqueue.QueueReference{Name: live.Jobs.Name, Type: rabbitmqqueue.QueueQuorum},
			Name:   "legacy-facade", Prefetch: 1, Concurrency: 1, HandlerTimeout: 10 * time.Second,
			MaxRequeues: 1, Failure: rabbitmqqueue.NegativeAcknowledge(true),
		},
		MessageID: func(core.TaskMessage) (string, error) { return "legacy-facade", nil },
	}
	worker, err := NewWorkerE(WithNativeConfig(config), WithQueue(live.Jobs.Name), WithTag("legacy-facade"))
	if err != nil {
		t.Fatalf("NewWorkerE() error = %v", err)
	}
	t.Cleanup(func() {
		if err := worker.Shutdown(); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})
	if worker.BackendName() != "rabbitmq" || worker.QueueName() != live.Jobs.Name {
		t.Fatalf("worker identity = (%q, %q)", worker.BackendName(), worker.QueueName())
	}
}
