package restutil

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
)

const Useragent = "up-restutil"

var uuidPattern = regexp.MustCompile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

func GetIDListRetriever(filePath string, URL string) IDListRetriever {
	if filePath != "" {
		return newFileBasedIDListRetriever(filePath)
	}
	return newURLBasedIDListRetriever(URL, HttpClient)
}

func newFileBasedIDListRetriever(filePath string) *fileBasedIDListRetriever {
	return &fileBasedIDListRetriever{filePath: filePath}
}

func newURLBasedIDListRetriever(baseURL string, client *http.Client) *urlBasedIDListRetriever {
	return &urlBasedIDListRetriever{
		baseURL: baseURL,
		client:  client}
}

//IDListRetriever is the interface used for retrieving UUIDs from a provided source
//
//Retrieve receives 2 unbuffered channels, one for IDs and the other for errors, and populates them accordingly as the
//function runs.
type IDListRetriever interface {
	Retrieve(chan<- string, chan<- error)
}

type fileBasedIDListRetriever struct {
	filePath string
}

func (r *fileBasedIDListRetriever) Retrieve(ids chan<- string, errChan chan<- error) {
	defer close(ids)
	inputFile, err := os.Open(r.filePath)
	if err != nil {
		errChan <- fmt.Errorf("ERROR - Failed opening file=%s: %s", r.filePath, err)
		return
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		if uuidPattern.MatchString(line) {
			ids <- line
		} else {
			errChan <- fmt.Errorf("ERROR - Found invalid ID=%s in file=%s", line, r.filePath)
			return
		}
	}
}

type urlBasedIDListRetriever struct {
	client  *http.Client
	baseURL string
}

func (r *urlBasedIDListRetriever) Retrieve(ids chan<- string, errChan chan<- error) {
	defer close(ids)
	u, err := url.Parse(r.baseURL)
	if err != nil {
		errChan <- fmt.Errorf("ERROR - %s", err)
		return
	}
	u, err = u.Parse("./__ids")
	if err != nil {
		errChan <- fmt.Errorf("ERROR - %s", err)
		return
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		errChan <- fmt.Errorf("ERROR - %s", err)
		return
	}
	req.Header.Set("User-Agent", Useragent)
	resp, err := r.client.Do(req)
	if err != nil {
		errChan <- fmt.Errorf("ERROR - %s", err)
		return
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
			errChan <- fmt.Errorf("ERROR - %s", err)
		}
		ids <- le.ID
	}
}
