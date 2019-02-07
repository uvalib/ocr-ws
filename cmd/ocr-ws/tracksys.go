package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// the line between metadata/masterfile fields is getting blurry; just lump them together
type tsGenericPidInfo struct {
	Id              int    `json:"id,omitempty"`
	Pid             string `json:"pid,omitempty"`
	Type            string `json:"type,omitempty"`
	Title           string `json:"title,omitempty"`
	Filename        string `json:"filename,omitempty"`
	TextSource      string `json:"text_source,omitempty"`
	OcrHint         string `json:"ocr_hint,omitempty"`
	OcrCandidate    bool   `json:"ocr_candidate,omitempty"`
	OcrLanguageHint string `json:"ocr_language_hint,omitempty"`
}

// holds metadata pid/page info
type tsPidInfo struct {
	Pid       tsGenericPidInfo
	Pages     []tsGenericPidInfo
	isOcrable bool
}

func tsApiUrlForPidUnit(api, pid, unit string) string {
	url := fmt.Sprintf("%s%s", config.tsApiHost.value, api)
	url = strings.Replace(url, "{PID}", pid, -1)

	if unit != "" {
		url = fmt.Sprintf("%s?unit=%s", url, unit)
	}

	logger.Printf("url: [%s]", url)

	return url
}

func tsGetPagesFromManifest(pid, unit string) ([]tsGenericPidInfo, error) {
	url := tsApiUrlForPidUnit(config.tsApiGetManifestTemplate.value, pid, unit)

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		logger.Printf("NewRequest() failed: %s", reqErr.Error())
		return nil, errors.New("Failed to create new manifest request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		logger.Printf("client.Do() failed: %s", resErr.Error())
		return nil, errors.New("Failed to receive manifest response")
	}

	defer res.Body.Close()

	// parse json from body

	var tsPages []tsGenericPidInfo

	buf, _ := ioutil.ReadAll(res.Body)
	if jErr := json.Unmarshal(buf, &tsPages); jErr != nil {
		logger.Printf("Unmarshal() failed: %s", jErr.Error())
		return nil, errors.New(fmt.Sprintf("Failed to unmarshal manifest response: [%s]", buf))
	}

	for i, p := range tsPages {
		logger.Printf("    [page %d / %d]  { [%s]  [%s]  [%s]  [%s] }", i+1, len(tsPages), p.Pid, p.Filename, p.Title, p.TextSource)
	}

	return tsPages, nil
}

func tsGetPidInfo(pid, unit string) (*tsPidInfo, error) {
	url := tsApiUrlForPidUnit(config.tsApiGetPidTemplate.value, pid, "")

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		logger.Printf("NewRequest() failed: %s", reqErr.Error())
		return nil, errors.New("Failed to create new pid request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		logger.Printf("client.Do() failed: %s", resErr.Error())
		return nil, errors.New("Failed to receive pid response")
	}

	defer res.Body.Close()

	// parse json from body

	var ts tsPidInfo

	buf, _ := ioutil.ReadAll(res.Body)
	if jErr := json.Unmarshal(buf, &ts.Pid); jErr != nil {
		logger.Printf("Unmarshal() failed: %s", jErr.Error())
		return nil, errors.New(fmt.Sprintf("Failed to unmarshal pid response: [%s]", buf))
	}

	logger.Printf("Type            : [%s]", ts.Pid.Type)
	logger.Printf("TextSource      : [%s]", ts.Pid.TextSource)
	logger.Printf("OcrHint         : [%s]", ts.Pid.OcrHint)
	logger.Printf("OcrCandidate    : [%t]", ts.Pid.OcrCandidate)
	logger.Printf("OcrLanguageHint : [%s]", ts.Pid.OcrLanguageHint)

	switch {
	case ts.Pid.Type == "master_file":
		logger.Printf("    [page 1 / 1]  { [%s]  [%s]  [%s]  [%s] }", ts.Pid.Pid, ts.Pid.Filename, ts.Pid.Title, ts.Pid.TextSource)

		ts.Pages = append(ts.Pages, ts.Pid)
		return &ts, nil

	case strings.Contains(ts.Pid.Type, "metadata"):
		var mfErr error

		ts.Pages, mfErr = tsGetPagesFromManifest(pid, unit)
		if mfErr != nil {
			logger.Printf("tsGetPagesFromManifest() failed: [%s]", mfErr.Error())
			return nil, mfErr
		}

		return &ts, nil
	}

	return nil, errors.New(fmt.Sprintf("Unhandled PID type: [%s]", ts.Pid.Type))
}

func tsGetMetadataPidInfo(pid, unit string) (*tsPidInfo, error) {
	ts, err := tsGetPidInfo(pid, unit)

	if err != nil {
		return nil, err
	}

	if strings.Contains(ts.Pid.Type, "metadata") == false {
		return nil, errors.New(fmt.Sprintf("PID is not a metadata type: [%s]", ts.Pid.Type))
	}

	// ensure there are pages to process
	if len(ts.Pages) == 0 {
		return nil, errors.New("Metadata PID does not have any pages")
	}

	// check if this is ocr-able: FIXME (DCMD-634)
	ts.isOcrable = false
	if ts.Pid.OcrCandidate == true {
		if ts.Pid.TextSource == "" || ts.Pid.TextSource == "ocr" {
			ts.isOcrable = true
		}
	} else {
		// fallback for tracksysdev until it has the new API
		if ts.Pid.OcrHint == "Regular Font" || ts.Pid.OcrHint == "Modern Font" {
			ts.isOcrable = true
		}
	}

	return ts, nil
}

func tsGetText(pid string) (string, error) {
	url := tsApiUrlForPidUnit(config.tsApiGetFullTextTemplate.value, pid, "")

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		logger.Printf("NewRequest() failed: %s", reqErr.Error())
		return "", errors.New("Failed to create new fulltext request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		logger.Printf("client.Do() failed: %s", resErr.Error())
		return "", errors.New("Failed to receive fulltext response")
	}

	defer res.Body.Close()

	// read text from body

	text, textErr := ioutil.ReadAll(res.Body)
	if textErr != nil {
		logger.Printf("ReadAll() failed: %s", textErr.Error())
		return "", errors.New("Failed to read fulltext response")
	}

	return string(text), nil
}

func textSnippet(text string) string {
	s := text

	s = strings.Replace(s, "\n", "\\n", -1)
	s = strings.Replace(s, "\r", "\\r", -1)
	s = strings.Replace(s, "\t", "\\t", -1)

	s = fmt.Sprintf("%-32s", s)

	return s[:32]
}

func tsPostText(pid, text string) error {
	// if url not set, just skip over this

	if config.tsApiPostFullTextTemplate.value == "" {
		//logger.Printf("SKIPPING TRACKSYS POST")
		return nil
	}

	form := url.Values{
		"text": {text},
	}

	url := tsApiUrlForPidUnit(config.tsApiPostFullTextTemplate.value, pid, "")

	req, reqErr := http.NewRequest("POST", url, strings.NewReader(form.Encode()))
	if reqErr != nil {
		logger.Printf("NewRequest() failed: %s", reqErr.Error())
		return errors.New("Failed to create new fulltext post request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		logger.Printf("client.Do() failed: %s", resErr.Error())
		return errors.New("Failed to receive fulltext post response")
	}

	defer res.Body.Close()

	buf, _ := ioutil.ReadAll(res.Body)
	logger.Printf("[%s] posted ocr: [%s...] (%d); response: [%s]", pid, textSnippet(text), len(text), buf)

	return nil
}

func tsJobStatusCallback(api, status, message, started, finished string) error {
	// { status: success/fail, message: informative message, started: start_time, finished: finish_time }

	jobstatus := struct {
		Status   string `json:"status,omitempty"`
		Message  string `json:"message,omitempty"`
		Started  string `json:"started,omitempty"`
		Finished string `json:"finished,omitempty"`
	}{
		Status:   status,
		Message:  message,
		Started:  started,
		Finished: finished,
	}

	output, jsonErr := json.Marshal(jobstatus)
	if jsonErr != nil {
		logger.Printf("Failed to serialize callback json: [%s]", jsonErr.Error())
		return errors.New("Failed to serialze job status callback json")
	}

	form := url.Values{
		"json": {string(output)},
	}

	url := tsApiUrlForPidUnit(api, "", "")

	req, reqErr := http.NewRequest("POST", url, strings.NewReader(form.Encode()))
	if reqErr != nil {
		logger.Printf("NewRequest() failed: %s", reqErr.Error())
		return errors.New("Failed to create new job status post request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		logger.Printf("client.Do() failed: %s", resErr.Error())
		return errors.New("Failed to receive job status post response")
	}

	defer res.Body.Close()

	buf, _ := ioutil.ReadAll(res.Body)
	logger.Printf("posted job status: [%s]; response: [%s]", string(output), buf)

	return nil
}
