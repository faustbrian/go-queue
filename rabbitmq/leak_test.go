package rabbitmq

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(testingMain *testing.M) {
	goleak.VerifyTestMain(testingMain)
}
