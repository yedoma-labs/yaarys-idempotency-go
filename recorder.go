package idempotency

import (
	"bytes"
	"net/http"
)

// responseRecorder captures the status, headers and body a handler writes. It implements
// http.ResponseWriter but does not forward anything to the client. The middleware writes the
// captured response afterwards, which keeps the first run and replays identical.
type responseRecorder struct {
	header      http.Header
	status      int
	wroteHeader bool
	body        bytes.Buffer
}

func newResponseRecorder() *responseRecorder {
	return &responseRecorder{header: make(http.Header)}
}

// Header returns the header map that will be stored with the response.
func (r *responseRecorder) Header() http.Header {
	return r.header
}

// WriteHeader records the status code. Only the first final status code counts. Informational
// 1xx responses are ignored.
func (r *responseRecorder) WriteHeader(code int) {
	if code >= 100 && code <= 199 {
		return
	}
	if r.wroteHeader {
		return
	}
	r.wroteHeader = true
	r.status = code
}

// Write appends to the captured body. Without an explicit status it records 200, as net/http does.
func (r *responseRecorder) Write(p []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.body.Write(p)
}

// response returns a deep copy of what was captured.
func (r *responseRecorder) response() StoredResponse {
	status := r.status
	if !r.wroteHeader {
		status = http.StatusOK
	}
	return StoredResponse{
		StatusCode: status,
		Header:     r.header.Clone(),
		Body:       bytes.Clone(r.body.Bytes()),
	}
}
