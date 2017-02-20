package restutil

import (
	"bytes"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

const (
	Ids = `{"id":"UUID-1"}
	{"id":"UUID-2"}
	{"id":"UUID-3"}
	{"id":"UUID-4"}
	{"id":"UUID-5"}
	{"id":"UUID-6"}
	{"id":"UUID-7"}
	{"id":"UUID-8"}
	{"id":"UUID-9"}
	{"id":"UUID-10"}`
	Payload             = "SomeData-%v"
	ExpectedContentType = "application/test"
)

func TestPutAllBinaryRest_allOK(t *testing.T) {
	user := "user"
	pass := "pass"
	conns := 1
	dumpFailed := false
	m := NewMockHttpServer()
	m.fResp <- Ids
	for i := 1; i < 11; i++ {
		m.fResp <- fmt.Sprintf(Payload, i)
	}

	defer m.Close()

	err := PutAllBinaryRest(m.from.URL, m.to.URL, user, pass, conns, 10, dumpFailed)
	assert.NoError(t, err)
	freqs := m.getFromReqs()
	assert.Equal(t, 11, len(freqs))
	assert.Equal(t, "/__ids", freqs[0].RequestURI)
	for i := 1; i < len(freqs); i++ {
		assert.True(t, strings.HasPrefix(freqs[i].RequestURI, "/UUID-"), "Request did not have prefix 'UUID-', RequestURI=%v", freqs[i].RequestURI)
		assert.Equal(t, "GET", freqs[i].Method)
	}

	treqs, tbdy := m.getToReqs()
	assert.Equal(t, 10, len(treqs))
	assert.Equal(t, 10, len(tbdy))
	for i := 0; i < 10; i++ {
		req := treqs[i]
		assert.Equal(t, "PUT", req.Method)
		is := strconv.FormatInt(int64(i+1), 10)
		assert.True(t, strings.HasPrefix(req.RequestURI, "/UUID-"+is), "Request did not have prefix 'UUID-%v', RequestURI=%v", is, req.RequestURI)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf(Payload, i+1), tbdy[i])
	}
}

func TestPutAllBinaryRest_allOKAndNoThrottling(t *testing.T) {
	user := "user"
	pass := "pass"
	conns := 1
	dumpFailed := false
	m := NewMockHttpServer()
	m.fResp <- Ids
	for i := 1; i < 11; i++ {
		m.fResp <- fmt.Sprintf(Payload, i)
	}

	defer m.Close()

	err := PutAllBinaryRest(m.from.URL, m.to.URL, user, pass, conns, 0, dumpFailed)
	assert.NoError(t, err)
	freqs := m.getFromReqs()
	assert.Equal(t, 11, len(freqs))
	assert.Equal(t, "/__ids", freqs[0].RequestURI)
	for i := 1; i < len(freqs); i++ {
		assert.True(t, strings.HasPrefix(freqs[i].RequestURI, "/UUID-"), "Request did not have prefix 'UUID-', RequestURI=%v", freqs[i].RequestURI)
		assert.Equal(t, "GET", freqs[i].Method)
	}

	treqs, tbdy := m.getToReqs()
	assert.Equal(t, 10, len(treqs))
	assert.Equal(t, 10, len(tbdy))
	for i := 0; i < 10; i++ {
		req := treqs[i]
		assert.Equal(t, "PUT", req.Method)
		assert.Equal(t, ExpectedContentType, req.Header.Get("Content-Type"))
		is := strconv.FormatInt(int64(i+1), 10)
		assert.True(t, strings.HasPrefix(req.RequestURI, "/UUID-"+is), "Request did not have prefix 'UUID-%v', RequestURI=%v", is, req.RequestURI)
	}
}

func TestPutAllBinaryRest_allOKWithMultipleConns(t *testing.T) {
	user := "user"
	pass := "pass"
	conns := 5
	dumpFailed := false
	m := NewMockHttpServer()
	m.fResp <- Ids
	for i := 1; i < 11; i++ {
		m.fResp <- fmt.Sprintf(Payload, i)
	}

	defer m.Close()

	err := PutAllBinaryRest(m.from.URL, m.to.URL, user, pass, conns, 5, dumpFailed)
	assert.NoError(t, err)
	freqs := m.getFromReqs()
	assert.Equal(t, 11, len(freqs))
	assert.Equal(t, "/__ids", freqs[0].RequestURI)

	treqs, tbdy := m.getToReqs()
	assert.Equal(t, 10, len(treqs))
	assert.Equal(t, 10, len(tbdy))
}

func TestPutAllBinaryRest_PutFails(t *testing.T) {
	user := "user"
	pass := "pass"
	conns := 1
	dumpFailed := true
	m := NewMockHttpServer()
	m.fResp <- Ids
	m.toError = true
	for i := 1; i < 11; i++ {
		m.fResp <- fmt.Sprintf(Payload, i)
	}

	defer m.from.Close()
	err := PutAllBinaryRest(m.from.URL, m.to.URL, user, pass, conns, 10, dumpFailed)
	assert.NoError(t, err)
	freqs := m.getFromReqs()
	assert.Equal(t, 11, len(freqs))
	assert.Equal(t, "/__ids", freqs[0].RequestURI)

	treqs, tbdy := m.getToReqs()
	assert.Equal(t, 10, len(treqs))
	assert.Equal(t, 10, len(tbdy))
}

func TestPutAllBinaryRest_PutFailsAndDoesNotDump(t *testing.T) {
	user := "user"
	pass := "pass"
	conns := 1
	dumpFailed := false
	m := NewMockHttpServer()
	m.fResp <- Ids
	m.toError = true
	for i := 1; i < 11; i++ {
		m.fResp <- fmt.Sprintf(Payload, i)
	}

	defer m.from.Close()
	err := PutAllBinaryRest(m.from.URL, m.to.URL, user, pass, conns, 10, dumpFailed)
	assert.NoError(t, err)
	freqs := m.getFromReqs()
	assert.Equal(t, 11, len(freqs))
	assert.Equal(t, "/__ids", freqs[0].RequestURI)

	treqs, tbdy := m.getToReqs()
	assert.Equal(t, 10, len(treqs))
	assert.Equal(t, 10, len(tbdy))
}

type mockHttpServer struct {
	sync.Mutex
	fResp     chan string
	fReqs     chan *http.Request
	tReqs     chan *http.Request
	tReqsBody chan string
	err       error
	from      *httptest.Server
	to        *httptest.Server
	toError   bool
}

func NewMockHttpServer() *mockHttpServer {
	m := mockHttpServer{
		fResp:     make(chan string, 100),
		fReqs:     make(chan *http.Request, 100),
		tReqs:     make(chan *http.Request, 100),
		tReqsBody: make(chan string, 100),
	}
	fs := httptest.NewServer(http.HandlerFunc(m.fakeFromHandlerFunc))
	ts := httptest.NewServer(http.HandlerFunc(m.fakeToHandlerFunc))
	m.from = fs
	m.to = ts
	return &m
}

func (m *mockHttpServer) fakeFromHandlerFunc(w http.ResponseWriter, r *http.Request) {
	m.fReqs <- r
	resp := <-m.fResp
	w.Header().Set("Content-Type", ExpectedContentType)
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, resp)
}

func (m *mockHttpServer) fakeToHandlerFunc(w http.ResponseWriter, r *http.Request) {
	m.tReqs <- r
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		log.Errorf("fakeTo=%v", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("{\"message\":\"some error\"}"))
		return
	}
	m.tReqsBody <- buf.String()
	if m.toError {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("{\"message\":\"some error\"}"))
		return
	}
	w.Header().Set("Content-Type", ExpectedContentType)
	w.WriteHeader(http.StatusCreated)
}

func (m *mockHttpServer) getFromReqs() []*http.Request {
	var reqs []*http.Request
	close(m.fReqs)
	for req := range m.fReqs {
		log.Infof("FRequestURI=%v", req.RequestURI)
		reqs = append(reqs, req)
	}
	return reqs
}

func (m *mockHttpServer) getToReqs() (hr []*http.Request, rb []string) {
	close(m.tReqs)
	close(m.tReqsBody)
	for req := range m.tReqs {
		log.Infof("TRequestURI=%v", req.RequestURI)
		hr = append(hr, req)
	}
	for b := range m.tReqsBody {
		log.Infof("TRequestBody=%v", b)
		rb = append(rb, b)
	}
	return hr, rb
}

func (m *mockHttpServer) Close() {
	m.to.Close()
	m.from.Close()
}
