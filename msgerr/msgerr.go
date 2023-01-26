package msgerr

type Error string

func (e Error) Error() string {
	return string(e)
}

const ErrorFileOpen = Error("failed opening file")
const ErrorFileClose = Error("failed closing file")
const ErrorFileCreate = Error("failed creating file")

const ErrorChunkTooSmall = Error("chunks size should be at least 1")
