package restutil

import (
	"errors"
	"fmt"
	"github.com/h2non/gock"
	"github.com/nbio/st"
	"github.com/stretchr/testify/assert"
	"net/http"
	"os"
	"testing"
)

func TestFileBasedRetrieve_Success(t *testing.T) {
	inputFilePath := "retriever_test"
	inputFile, err := os.OpenFile(inputFilePath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Errorf("Failed opening test file=%s, %s", inputFilePath, err)
		return
	}
	defer os.Remove(inputFilePath)

	var expectedIds []string
	expectedIds = append(expectedIds, "2d3e16e0-61cb-4322-8aff-3b01c59f4daa", "fd4459b2-cc4e-4ec8-9853-c5238eb860fb")

	for _, id := range expectedIds {
		inputFile.WriteString(id)
		inputFile.WriteString("\n")
	}
	inputFile.Close()

	retriever := newFileBasedIDListRetriever(inputFilePath)
	var idsChan = make(chan string)
	var errChan = make(chan error)
	var actualIds = make(map[string]struct{})

	go retriever.Retrieve(idsChan, errChan)

	for idsChan != nil {
		select {
		case id, ok := <-idsChan:
			if !ok {
				idsChan = nil
			} else {
				actualIds[id] = struct{}{}
			}
		case err := <-errChan:
			t.Errorf("Error not expected: %s", err)
			return
		}
	}

	for _, id := range expectedIds {
		if _, found := actualIds[id]; !found {
			st.Expect(t, true, found)
			return
		}
	}
}

func TestFileBasedRetrieve_FileOpenFailure(t *testing.T) {
	expectedError := "ERROR - Failed opening file=non_existing_file:"
	retriever := newFileBasedIDListRetriever("non_existing_file")
	var idsChan = make(chan string)
	var errChan = make(chan error)

	go retriever.Retrieve(idsChan, errChan)

	for idsChan != nil {
		select {
		case _, ok := <-idsChan:
			if !ok {
				idsChan = nil
			}
		case actualError := <-errChan:
			assert.Contains(t, actualError.Error(), expectedError)
			return
		}
	}
	t.Error("Expected error but no error was returned")
}

func TestFileBasedRetrieve_InvalidIdFailure(t *testing.T) {
	var invalidIds []string
	invalidIds = append(
		invalidIds,
		"2d3e16e-61cb-4322-8aff-3b01c59f4daa",
		"2d3e16e0-1cb-4322-8aff-3b01c59f4daa",
		"2d3e16e0-61cb-322-8aff-3b01c59f4daa",
		"2d3e16e0-61cb-4322-aff-3b01c59f4daa",
		"2d3e16e0-61cb-4322-8aff-3b01c59f4da",
		"2d3e16e0-61cb-4322-8aff-3b01c59f4da",
		"2d3e16e0-61cb-4322-8aff-3b01c59f4da",
		"2d3e16e0-61cb-4322-8aff-3b01c59f4da",
		"2d3e16e0-61cb-4322-8aff-3b01c59f4da",
		"2d3e16e0-61cb-4322-8aff-3b01c59f4da",
		"gd3e16e0-61cb-4322-8aff-3b01c59f4daa",
		"2d3e16e0-61cb-4322-8aff-3b01c59f4daa-",
		"-2d3e16e0-61cb-4322-8aff-3b01c59f4daa")

	for _, invalidID := range invalidIds {
		retrieveInvalidID(t, invalidID)
	}
}

func retrieveInvalidID(t *testing.T, invalidID string) {
	expectedError := fmt.Errorf("ERROR - Found invalid ID=%s in file=retriever_test", invalidID)
	inputFilePath := "retriever_test"
	inputFile, err := os.OpenFile(inputFilePath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		t.Errorf("Failed opening test file=%s, %s", inputFilePath, err)
		return
	}
	defer os.Remove(inputFilePath)

	inputFile.WriteString(invalidID)
	inputFile.WriteString("\n")
	inputFile.Close()

	retriever := newFileBasedIDListRetriever(inputFilePath)
	var idsChan = make(chan string)
	var errChan = make(chan error)

	go retriever.Retrieve(idsChan, errChan)

	for idsChan != nil {
		select {
		case _, ok := <-idsChan:
			if !ok {
				idsChan = nil
			}
		case actualError := <-errChan:
			if expectedError != actualError {
				st.Expect(t, expectedError, actualError)
			}
			return
		}
	}
	t.Error("Expected error but no error was returned")
}

func TestUrlBasedRetrieve_Success(t *testing.T) {
	defer gock.Off()
	gock.New("http://localhost").
		Get("/endpoint/__ids").
		Reply(200).
		JSON(map[string]string{"id": "c0de16de-00e6-3d52-aca5-c2a300cd1144"})

	var expectedIds []string
	expectedIds = append(expectedIds, "c0de16de-00e6-3d52-aca5-c2a300cd1144")

	retriever := newURLBasedIDListRetriever("http://localhost/endpoint/", http.DefaultClient)
	var idsChan = make(chan string)
	var errChan = make(chan error)
	var actualIds = make(map[string]struct{})

	go retriever.Retrieve(idsChan, errChan)

	for idsChan != nil {
		select {
		case id, ok := <-idsChan:
			if !ok {
				idsChan = nil
			} else {
				actualIds[id] = struct{}{}
			}
		case err := <-errChan:
			t.Errorf("Error not expected: %s", err)
			return
		}
	}

	for _, id := range expectedIds {
		if _, found := actualIds[id]; !found {
			st.Expect(t, true, found)
			return
		}
	}

	st.Expect(t, gock.IsDone(), true)
}

func TestUrlBasedRetrieve_InvalidUrlFailure(t *testing.T) {
	expectedError := errors.New("ERROR - parse :http//localhost/endpoint/: missing protocol scheme")
	retriever := newURLBasedIDListRetriever(":http//localhost/endpoint/", http.DefaultClient)
	var idsChan = make(chan string)
	var errChan = make(chan error)

	go retriever.Retrieve(idsChan, errChan)

	for idsChan != nil {
		select {
		case _, ok := <-idsChan:
			if !ok {
				idsChan = nil
			}
		case actualError := <-errChan:
			if expectedError != actualError {
				st.Expect(t, expectedError, actualError)
			}
			return
		}
	}

	st.Expect(t, gock.IsDone(), true)
}

func TestUrlBasedRetrieve_InvalidResponseFailure(t *testing.T) {
	defer gock.Off()
	gock.New("http://localhost").
		Get("/endpoint/__ids").
		Reply(200).BodyString("Invalid response")
	expectedError := errors.New("ERROR - invalid character 'I' looking for beginning of value")

	retriever := newURLBasedIDListRetriever("http://localhost/endpoint/", http.DefaultClient)
	var idsChan = make(chan string)
	var errChan = make(chan error)

	go retriever.Retrieve(idsChan, errChan)

	for idsChan != nil {
		select {
		case _, ok := <-idsChan:
			if !ok {
				idsChan = nil
			}
		case actualError := <-errChan:
			if expectedError != actualError {
				st.Expect(t, expectedError, actualError)
			}
			return
		}
	}

	st.Expect(t, gock.IsDone(), true)
}
