package msgerr

type MyError string

func (e MyError) Error() string {
	return string(e)
}

const (
	ErrorFileOpen   = MyError("failed opening file")
	ErrorFileClose  = MyError("failed closing file")
	ErrorFileCreate = MyError("failed creating file")

	ErrorChunkTooSmall = MyError("chunk size should be at least 1")
)
