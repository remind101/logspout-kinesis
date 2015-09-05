package kineprod

type flusher interface {
	flush([]byte)
}
