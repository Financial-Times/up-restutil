package restutil

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
	"gopkg.in/cheggaaa/pb.v1"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	Transport = &http.Transport{
		MaxIdleConnsPerHost: 128,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	}

	HttpClient = &http.Client{
		Transport: Transport,
	}
)

const (
	BufferSize = 24
)

type binaryMsg struct {
	id   *string
	body *io.ReadCloser
	ct   string
}

func PutAllBinaryRest(baseFromURL string, baseToURL string, user string, pass string, conns int, throttle int, dumpFailed bool) (err error) {
	msgs := make(chan *binaryMsg, 128)
	var failChan chan []byte
	rp := &resourcePutter{
		baseURL: baseToURL,
		user:    user,
		pass:    pass,
	}

	errs := make(chan error)
	failwg := sync.WaitGroup{}
	if dumpFailed {
		failChan = make(chan []byte, conns*2)
		failwg.Add(1)
		go func() {
			defer failwg.Done()

			for resource := range failChan {
				_, err := os.Stdout.Write(resource)
				if err == nil {
					_, err = io.WriteString(os.Stdout, "\n")
				}
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
			}
		}()

	}

	var wg sync.WaitGroup

	for i := 0; i < conns; i++ {
		wg.Add(1)
		go rp.putAllBinary(msgs, failChan, &wg)
	}
	getAllBinary(baseFromURL, throttle, conns, msgs)
	wg.Wait()

	if dumpFailed {
		close(failChan)
		failwg.Wait()
	}

	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func getAllBinary(baseURL string, throttle int, conns int, msgs chan *binaryMsg) (err error) {
	ids := make(chan *string, conns*BufferSize)
	go fetchIDList(baseURL, ids)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	var rl *rate.Limiter
	if throttle > 0 {
		rl = rate.NewLimiter(rate.Limit(throttle), 1)
	}

	for i := 0; i < conns; i++ {
		wg.Add(1)
		go fetchBinaryMessage(baseURL, ids, msgs, rl, &ctx, &wg)
	}
	wg.Wait()
	close(msgs)
	return err
}

func fetchBinaryMessage(baseURL string, ids chan *string, msgs chan<- *binaryMsg, lim *rate.Limiter, ctx *context.Context, wg *sync.WaitGroup) {
	for id := range ids {
		log.Infof("Fetching ID=%v", *id)
		var err error
		if lim != nil {
			if err := lim.Wait(*ctx); err != nil {
				log.Errorf("Got error rate limiting, %v", err.Error())
				ids <- id
				continue
			}

		}

		reqURI, _ := generatePutURL(*id, baseURL)
		req, err := http.NewRequest("GET", reqURI.String(), nil)
		if err != nil {
			log.Errorf("Got error creating NewRequest, %v", err.Error())
			continue
		}

		req.Header.Set("User-Agent", Useragent)
		resp, err := HttpClient.Do(req)
		if err != nil {
			log.Errorf("Got error making request, %v", err.Error())
			resp.Body.Close()
			continue
		}

		msg := &binaryMsg{
			id:   id,
			body: &resp.Body,
			ct:   resp.Header.Get("Content-Type"),
		}

		msgs <- msg

	}
	wg.Done()
}

func PutAllRest(baseURL string, idProperty string, user string, pass string, conns int, dumpFailed bool) error {

	dec := json.NewDecoder(os.Stdin)

	docs := make(chan resource)

	Transport.MaxIdleConnsPerHost = conns

	rp := &resourcePutter{baseURL, idProperty, user, pass}

	errs := make(chan error, 1)

	var failChan chan []byte

	failwg := sync.WaitGroup{}

	if dumpFailed {
		failChan = make(chan []byte)

		failwg.Add(1)
		go func() {
			defer failwg.Done()

			for resource := range failChan {
				_, err := os.Stdout.Write(resource)
				if err == nil {
					_, err = io.WriteString(os.Stdout, "\n")
				}
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
			}
		}()
	}

	wg := sync.WaitGroup{}

	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func() {
			if err := rp.putAll(docs, failChan); err != nil {
				select {
				case errs <- err:
				default:
				}
			}
			wg.Done()
		}()
	}

	for {
		var doc map[string]interface{}
		if err := dec.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		select {
		case docs <- doc:
		case err := <-errs:
			return err
		}
	}

	close(docs)

	wg.Wait()

	if dumpFailed {
		close(failChan)
		failwg.Wait()
	}

	select {
	case err := <-errs:
		return err
	default:
		return nil
	}

}

func DiffIDs(sourceURL, destURL string) error {
	sourceIDs := make(chan *string)
	go fetchIDList(sourceURL, sourceIDs)

	destIDs := make(chan *string)
	go fetchIDList(destURL, destIDs)

	sources := make(map[string]struct{})
	dests := make(map[string]struct{})

	for sourceIDs != nil || destIDs != nil {
		select {
		case sourceID, ok := <-sourceIDs:
			if !ok {
				sourceIDs = nil
			} else {
				sources[*sourceID] = struct{}{}
			}
		case destID, ok := <-destIDs:
			if !ok {
				destIDs = nil
			} else {
				dests[*destID] = struct{}{}
			}
		}
	}

	var output struct {
		OnlyInSource      []string `json:"only-in-source"`
		OnlyInDestination []string `json:"only-in-destination"`
	}

	output.OnlyInSource = []string{}
	output.OnlyInDestination = []string{}

	for s := range sources {
		if _, found := dests[s]; !found {
			output.OnlyInSource = append(output.OnlyInSource, s)
		} else {
			delete(dests, s)
		}

	}

	for s := range dests {
		output.OnlyInDestination = append(output.OnlyInDestination, s)
	}

	return json.NewEncoder(os.Stdout).Encode(output)

}

type SyncService struct {
	SourceIDsRetriever IDListRetriever
	DestIDsRetriever   IDListRetriever
	SourceURL          string
	DestURL            string
	MaxConcurrentReqs  int
	MinExecTime        int
	Retries            int
	Deletes            bool
}

func SyncIDs(service *SyncService) error {
	errChan := make(chan error)
	defer close(errChan)
	sourceIDs := make(chan string)
	go service.SourceIDsRetriever.Retrieve(sourceIDs, errChan)
	destIDs := make(chan string)
	go service.DestIDsRetriever.Retrieve(destIDs, errChan)

	sources := make(map[string]struct{})
	dests := make(map[string]struct{})

	for sourceIDs != nil || destIDs != nil {
		select {
		case sourceID, ok := <-sourceIDs:
			if !ok {
				sourceIDs = nil
			} else {
				sources[sourceID] = struct{}{}
			}
		case destID, ok := <-destIDs:
			if !ok {
				destIDs = nil
			} else {
				dests[destID] = struct{}{}
			}
		case err := <-errChan:
			return err
		}
	}

	var output struct {
		Created int `json:"created"`
		Deleted int `json:"created"`
	}

	sem := make(chan struct{}, service.MaxConcurrentReqs)
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}

	errs := make(chan error, 1)

	if len(sources) > 0 {
		var wg sync.WaitGroup
		bar := pb.StartNew(len(sources))

		for s := range sources {
			if _, found := dests[s]; !found {
				select {
				case err := <-errs:
					return err
				default:
					<-sem
					wg.Add(1)
					go func(id string) {
						defer func() {
							sem <- struct{}{}
							wg.Done()
						}()
						minExecTime := time.After(time.Second * time.Duration(service.MinExecTime))
						retry := service.Retries
						for {
							if err := doCopy(service.SourceURL, service.DestURL, id); err != nil {
								if retry == 0 {
									errs <- err
									break
								} else {
									retry--
									time.Sleep(time.Second * 2)
								}
							} else {
								break
							}
						}
						<-minExecTime
					}(s)
					output.Created++
					bar.Increment()
				}
			} else {
				bar.Increment()
				delete(dests, s)
			}

		}
		wg.Wait()
		bar.FinishPrint("Done creates")
	}

	if service.Deletes && len(dests) > 0 {
		bar := pb.StartNew(len(dests))

		for s := range dests {
			if err := doDelete(service.DestURL, s); err != nil {
				return err
			}
			output.Deleted++
			bar.Increment()
		}
		bar.FinishPrint("Done deletes")
	}

	return json.NewEncoder(os.Stdout).Encode(output)
}

func doCopy(sourceURL, destURL, id string) error {

	su := sourceURL
	if !strings.HasSuffix(su, "/") {
		su = su + "/"
	}

	sreq, err := http.NewRequest("GET", fmt.Sprintf("%s%s", su, id), nil)
	if err != nil {
		return err
	}
	sreq.Header.Set("User-Agent", Useragent)
	sresp, err := HttpClient.Do(sreq)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, sresp.Body)
		_ = sresp.Body.Close()
	}()

	if sresp.StatusCode != http.StatusOK {
		return fmt.Errorf("error copying resource: %s", sresp.Status)
	}

	du := destURL
	if !strings.HasSuffix(du, "/") {
		du = du + "/"
	}

	dreq, err := http.NewRequest("PUT", fmt.Sprintf("%s%s", du, id), sresp.Body)
	if err != nil {
		return err
	}
	dreq.Header.Set("User-Agent", Useragent)
	dreq.Header.Set("Content-type", "application/json")
	dresp, err := HttpClient.Do(dreq)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, dresp.Body)
		_ = dresp.Body.Close()
	}()
	if dresp.StatusCode != http.StatusOK {
		return fmt.Errorf("error copying resource: %s", dresp.Status)
	}

	return nil
}

func doDelete(destURL, id string) error {

	du := destURL
	if !strings.HasSuffix(du, "/") {
		du = du + "/"
	}

	dreq, err := http.NewRequest("DELETE", fmt.Sprintf("%s%s", du, id), nil)
	if err != nil {
		return err
	}
	dresp, err := HttpClient.Do(dreq)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, dresp.Body)
		_ = dresp.Body.Close()
	}()
	if dresp.StatusCode != http.StatusOK {
		return fmt.Errorf("error deleting resource: %s", dresp.Status)
	}
	return nil
}

func (rp *resourcePutter) putAllBinary(msgs <-chan *binaryMsg, failChan chan []byte, wg *sync.WaitGroup) {
	for msg := range msgs {
		putURL, err := generatePutURL(*msg.id, rp.baseURL)

		if err != nil {
			log.Errorf("generatePutURL Error=%v", err.Error())
			if failChan != nil {
				failChan <- []byte(*msg.id)
			}
			continue
		}

		var r io.Reader = *msg.body
		err = rp.put(putURL.String(), r, msg.ct)

		if err != nil {
			log.Errorf("PUT putURL=%v, Error=%v", putURL, err.Error())
			if failChan != nil {
				failChan <- []byte(*msg.id)
			}
			(*msg.body).Close()
			continue
		}
		(*msg.body).Close()
	}
	wg.Done()
}

func generatePutURL(id string, baseURL string) (putURL *url.URL, err error) {
	if !strings.HasSuffix(baseURL, "/") {
		baseURL = baseURL + "/"
	}
	putURL, err = url.Parse(baseURL)
	if err != nil {
		return
	}
	putURL, err = putURL.Parse(id)
	return
}

func (rp *resourcePutter) putAll(resources <-chan resource, failChan chan []byte) error {
	for r := range resources {
		id := r[rp.idProperty]
		idStr, ok := id.(string)
		if !ok {
			log.Info("unable to extract id property from resource, skipping")
		}

		msg, err := json.Marshal(r)
		if err != nil {
			return err
		}
		u, err := generatePutURL(idStr, rp.baseURL)
		if err != nil {
			return err
		}
		err = rp.put(u.String(), bytes.NewReader(msg), "application/json")
		if err != nil {
			if failChan != nil {
				failChan <- msg
			} else {
				return err
			}
		}
	}
	return nil
}

func (rp *resourcePutter) put(url string, data io.Reader, contentType string) (err error) {
	req, err := http.NewRequest("PUT", url, data)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", Useragent)
	req.Header.Set("Content-Type", contentType)

	if rp.user != "" && rp.pass != "" {
		req.SetBasicAuth(rp.user, rp.pass)
	}
	resp, err := HttpClient.Do(req)
	if err != nil {
		return
	}
	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	resp.Body.Close()
	if resp.StatusCode > 299 {
		return fmt.Errorf("http fail: %v :\n%s\n", resp.Status, contents)
	}

	return
}

func GetAllRest(baseURL string, throttle int) (err error) {
	messages := make(chan string, 128)

	go getAllRest(baseURL, throttle, messages)

	for msg := range messages {
		log.Info(msg)
	}
	return err
}

func getAllRest(baseURL string, throttle int, messages chan string) (err error) {
	log.Infof("baseURL=%v throttle=%v", baseURL, throttle)
	if baseURL == "" {
		return errors.New("baseURL must be provided")
	}
	if !strings.HasSuffix(baseURL, "/") {
		baseURL = baseURL + "/"
	}
	if throttle < 1 {
		log.Fatalf("Invalid throttle %d", throttle)
	}
	ticker := time.NewTicker(time.Second / time.Duration(throttle))

	go func() {
		fetchAll(baseURL, messages, ticker)
		close(messages)
	}()
	return err
}

func fetchAll(baseURL string, messages chan<- string, ticker *time.Ticker) {
	ids := make(chan *string, 128)
	go fetchIDList(baseURL, ids)

	readers := 32

	Transport.MaxIdleConnsPerHost = readers

	readWg := sync.WaitGroup{}

	for i := 0; i < readers; i++ {
		readWg.Add(1)
		go func(i int) {
			fetchMessages(baseURL, messages, ids, ticker)
			readWg.Done()
		}(i)
	}

	readWg.Wait()
}

func fetchIDList(baseURL string, ids chan<- *string) {

	u, err := url.Parse(baseURL)
	if err != nil {
		log.Fatal(err)
	}
	u, err = u.Parse("./__ids")
	if err != nil {
		log.Fatal(err)
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("User-Agent", Useragent)
	resp, err := HttpClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	type listEntry struct {
		ID string `json:"id"`
	}

	var le listEntry
	dec := json.NewDecoder(resp.Body)
	for {
		err = dec.Decode(&le)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		var id = le.ID
		ids <- &id
	}

	close(ids)
}

func fetchMessages(baseURL string, messages chan<- string, ids <-chan *string, ticker *time.Ticker) {
	for id := range ids {
		<-ticker.C
		req, err := http.NewRequest("GET", strings.Join([]string{baseURL, *id}, ""), nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("User-Agent", Useragent)
		resp, err := HttpClient.Do(req)
		if err != nil {
			panic(err)
		}
		data, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			panic(err)
		}
		messages <- string(data)
	}
}

type resource map[string]interface{}

type resourcePutter struct {
	baseURL    string
	idProperty string
	user       string
	pass       string
}
