package queueservice

import (
	"errors"
	"testing"

	successor "github.com/faustbrian/go-queue/adapters/service"
)

func FuzzLegacyStructuredErrorTranslation(f *testing.F) {
	f.Add(uint8(0), uint8(1), "field", "reason")
	f.Add(uint8(4), uint8(3), "", "")
	f.Fuzz(func(t *testing.T, kind, value uint8, field, reason string) {
		const maximumTextBytes = 256
		if len(field) > maximumTextBytes {
			field = field[:maximumTextBytes]
		}
		if len(reason) > maximumTextBytes {
			reason = reason[:maximumTextBytes]
		}
		cause := errors.New("cause")
		var source error
		var target any
		switch kind % 5 {
		case 0:
			source = &successor.OptionsError{Field: field, Reason: reason}
			target = new(*OptionsError)
		case 1:
			source = &successor.StartupError{Validation: cause, Cleanup: cause}
			target = new(*StartupError)
		case 2:
			source = &successor.PublishError{Acceptance: successor.PublishAcceptance(value), Err: cause}
			target = new(*PublishError)
		case 3:
			source = &successor.CallbackPanicError{Operation: successor.CallbackOperation(value)}
			target = new(*CallbackPanicError)
		default:
			source = &successor.CallbackError{Operation: successor.CallbackOperation(value), Err: cause}
			target = new(*CallbackError)
		}
		converted := compatibilityError(source)
		if !errors.As(converted, target) {
			t.Fatalf("compatibilityError(%T) = %T", source, converted)
		}
		if converted.Error() != source.Error() {
			t.Fatalf("error text changed: %q != %q", converted, source)
		}
		var leakedOptions *successor.OptionsError
		var leakedStartup *successor.StartupError
		var leakedPublish *successor.PublishError
		var leakedCallback *successor.CallbackError
		var leakedPanic *successor.CallbackPanicError
		if errors.As(converted, &leakedOptions) || errors.As(converted, &leakedStartup) ||
			errors.As(converted, &leakedPublish) || errors.As(converted, &leakedCallback) ||
			errors.As(converted, &leakedPanic) {
			t.Fatal("legacy error graph leaked a successor structured error")
		}
	})
}
