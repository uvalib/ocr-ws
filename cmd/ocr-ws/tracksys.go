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

// fields common to metadata, master_file, and cloned_from structs
type tsAPICommonFields struct {
	Id         int    `json:"id,omitempty"`
	Pid        string `json:"pid,omitempty"`
	Type       string `json:"type,omitempty"`
	Title      string `json:"title,omitempty"`
	Filename   string `json:"filename,omitempty"`
	TextSource string `json:"text_source,omitempty"`
}

// /api/pid/:PID response

type tsAPIPidClonedFrom struct {
	// only uses id/pid/filename
	tsAPICommonFields
}

type tsAPIPidResponse struct {
	tsAPICommonFields
	// in metadata only:
	AvailabilityPolicy string `json:"availability_policy,omitempty"`
	OcrHint            string `json:"ocr_hint,omitempty"`
	OcrLanguageHint    string `json:"ocr_language_hint,omitempty"`
	// in master_files only:
	ParentMetadataPid string             `json:"parent_metadata_pid,omitempty"`
	ClonedFrom        tsAPIPidClonedFrom `json:"cloned_from,omitempty"`
}

// /api/manifest/:PID response

type tsAPIManifestFile struct {
	tsAPICommonFields
	Width  int `json:"width,omitempty"`
	Height int `json:"height,omitempty"`
}

// unused because json response is unnamed array
type tsAPIManifestResponse struct {
	Files []tsAPIManifestFile
}

// holds pid/page info
type tsPidInfo struct {
	tsAPIPidResponse
	Pages []tsAPICommonFields
}

func tsGetPagesFromManifest(ocr ocrInfo, w http.ResponseWriter) ([]tsAPICommonFields, error) {
	url := fmt.Sprintf("%s%s", config.tsApiHost.value, config.tsApiGetManifestTemplate.value)
	url = strings.Replace(url, "{PID}", ocr.req.pid, 1)

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

	var tsManifestInfo []tsAPIManifestFile

	buf, _ := ioutil.ReadAll(res.Body)
	if jErr := json.Unmarshal(buf, &tsManifestInfo); jErr != nil {
		logger.Printf("Unmarshal() failed: %s", jErr.Error())
		return nil, errors.New(fmt.Sprintf("Failed to unmarshal manifest response: [%s]", buf))
	}

	var tsPages []tsAPICommonFields

	for i, p := range tsManifestInfo {
		logger.Printf("    [page %d / %d]  { [%s]  [%s]  [%s] }", i+1, len(tsManifestInfo), p.Pid, p.Filename, p.Title)

		tsPages = append(tsPages, tsAPICommonFields{Pid: p.Pid, Filename: p.Filename, Title: p.Title})
	}

	return tsPages, nil
}

func tsGetPidInfo(ocr ocrInfo, w http.ResponseWriter) (*tsPidInfo, error) {
	url := fmt.Sprintf("%s%s", config.tsApiHost.value, config.tsApiGetPidTemplate.value)
	url = strings.Replace(url, "{PID}", ocr.req.pid, 1)

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
	if jErr := json.Unmarshal(buf, &ts); jErr != nil {
		logger.Printf("Unmarshal() failed: %s", jErr.Error())
		return nil, errors.New(fmt.Sprintf("Failed to unmarshal pid response: [%s]", buf))
	}

	logger.Printf("Type               : [%s]", ts.Type)
	logger.Printf("AvailabilityPolicy : [%s]", ts.AvailabilityPolicy)
	logger.Printf("ParentMetadataPid  : [%s]", ts.ParentMetadataPid)
	logger.Printf("TextSource         : [%s]", ts.TextSource)
	logger.Printf("OcrHint            : [%s]", ts.OcrHint)
	logger.Printf("OcrLanguageHint    : [%s]", ts.OcrLanguageHint)

	switch {
	case ts.Type == "master_file":
		logger.Printf("    [page 1 / 1]  { [%s]  [%s]  [%s] }", ts.Pid, ts.Filename, ts.Title)

		ts.Pages = append(ts.Pages, tsAPICommonFields{Id: ts.Id, Pid: ts.Pid, Type: ts.Type, Filename: ts.Filename, Title: ts.Title, TextSource: ts.TextSource})
		return &ts, nil

	case strings.Contains(ts.Type, "metadata"):
		var mfErr error
		ts.Pages, mfErr = tsGetPagesFromManifest(ocr, w)
		if mfErr != nil {
			logger.Printf("tsGetPagesFromManifest() failed: [%s]", mfErr.Error())
			return nil, mfErr
		}
		return &ts, nil
	}

	return nil, errors.New(fmt.Sprintf("Unhandled PID type: [%s]", ts.Type))
}

func tsGetText(pid string) (string, error) {
	url := fmt.Sprintf("%s%s", config.tsApiHost.value, config.tsApiGetFullTextTemplate.value)
	url = strings.Replace(url, "{PID}", pid, 1)

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

func tsPostText(pid, text string) error {
	// if url not set, just skip over this

	if config.tsApiPostFullTextTemplate.value == "" {
		//logger.Printf("SKIPPING TRACKSYS POST")
		return nil
	}

	form := url.Values{
		"text": {text},
	}

	url := fmt.Sprintf("%s%s", config.tsApiHost.value, config.tsApiPostFullTextTemplate.value)
	url = strings.Replace(url, "{PID}", pid, 1)

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

	//buf, _ := ioutil.ReadAll(res.Body)
	//logger.Printf("Posted OCR; response: [%s]", buf)

	return nil
}
