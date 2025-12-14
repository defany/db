package txman

type HandledPanic struct {
	Err        error
	Stacktrace string
}
