package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

// fields common to multiple API response structs

type tsAPICommonFields struct {
	Id       int    `json:"id,omitempty"`
	Pid      string `json:"pid,omitempty"`
	Title    string `json:"title,omitempty"`
	Filename string `json:"filename,omitempty"`
}

// /api/pid/:PID response

type tsAPIPidClonedFrom struct {
	tsAPICommonFields
}

type tsAPIPidResponse struct {
	tsAPICommonFields
	Type               string             `json:"type,omitempty"`
	AvailabilityPolicy string             `json:"availability_policy,omitempty"`
	TextSource         string             `json:"text_source,omitempty"`
	OcrHint            string             `json:"ocr_hint,omitempty"`
	OcrLanguageHint    string             `json:"ocr_language_hint,omitempty"`
	ClonedFrom         tsAPIPidClonedFrom `json:"cloned_from,omitempty"`
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

func tsGetPagesFromManifest(ocr ocrInfo, w http.ResponseWriter) (pages []pageInfo, err error) {
	url := strings.Replace(config.tsGetManifestUrlTemplate.value, "{PID}", ocr.req.pid, 1)

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		logger.Printf("NewRequest() failed: %s", reqErr.Error())
		return nil, errors.New("Failed to create new manifest request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		logger.Printf("client.Do() failed: %s", resErr.Error())
		return nil, errors.New("Failed to read manifest response")
	}

	defer res.Body.Close()

	// parse json from body

	var tsManifestInfo []tsAPIManifestFile

	buf, _ := ioutil.ReadAll(res.Body)
	if jErr := json.Unmarshal(buf, &tsManifestInfo); jErr != nil {
		logger.Printf("Unmarshal() failed: %s", jErr.Error())
		return nil, errors.New(fmt.Sprintf("Failed to unmarshal manifest response: [%s]", buf))
	}

	for _, p := range tsManifestInfo {
		pages = append(pages, pageInfo{PID: p.Pid, Filename: p.Filename, Title: p.Title})
	}

	return pages, nil
}

func tsGetPages(ocr ocrInfo, w http.ResponseWriter) (pages []pageInfo, err error) {
	url := strings.Replace(config.tsGetPidUrlTemplate.value, "{PID}", ocr.req.pid, 1)

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		logger.Printf("NewRequest() failed: %s", reqErr.Error())
		return nil, errors.New("Failed to create new pid request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		logger.Printf("client.Do() failed: %s", resErr.Error())
		return nil, errors.New("Failed to read pid response")
	}

	defer res.Body.Close()

	// parse json from body

	var tsPidInfo tsAPIPidResponse

	buf, _ := ioutil.ReadAll(res.Body)
	if jErr := json.Unmarshal(buf, &tsPidInfo); jErr != nil {
		logger.Printf("Unmarshal() failed: %s", jErr.Error())
		return nil, errors.New(fmt.Sprintf("Failed to unmarshal pid response: [%s]", buf))
	}

	logger.Printf("Type               : [%s]", tsPidInfo.Type)
	logger.Printf("AvailabilityPolicy : [%s]", tsPidInfo.AvailabilityPolicy)
	logger.Printf("TextSource         : [%s]", tsPidInfo.TextSource)
	logger.Printf("OcrHint            : [%s]", tsPidInfo.OcrHint)
	logger.Printf("OcrLanguageHint    : [%s]", tsPidInfo.OcrLanguageHint)

	switch {
	case strings.Contains(tsPidInfo.Type, "metadata"):
		return tsGetPagesFromManifest(ocr, w)
	case tsPidInfo.Type == "component":
		return nil, errors.New("PID is a component")
	}

	pages = append(pages, pageInfo{PID: tsPidInfo.Pid, Filename: tsPidInfo.Filename, Title: tsPidInfo.Title})

	return pages, nil
}

func tsGetText(pid string) (error, string) {
	return nil, "blah"
}

func tsPostText(pid, text string) error {
	return nil
}
