package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
)

type ocrRequest struct {
	pid      string
	unit     string
	email    string
	callback string
	force    string
	lang     string
}

type ocrInfo struct {
	req     ocrRequest // values from original request
	ts      *tsPidInfo // values looked up in tracksys
	subDir  string
	workDir string
	reqID   string
}

/**
 * Handle a request for OCR of page images
 */
func ocrGenerateHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	logger.Printf("%s %s", r.Method, r.RequestURI)

	ocr := ocrInfo{}

	// save fields from original request
	ocr.req.pid = params.ByName("pid")
	ocr.req.unit = r.URL.Query().Get("unit")
	ocr.req.email = r.URL.Query().Get("email")
	ocr.req.callback = r.URL.Query().Get("callback")
	ocr.req.force = r.URL.Query().Get("force")
	ocr.req.lang = r.URL.Query().Get("lang")

	// save info generated from the original request
	ocr.subDir = ocr.req.pid
	ocr.workDir = getWorkDir(ocr.subDir)
	ocr.reqID = randomID()

	// check if forcing ocr... bypasses all checks except pid existence (e.g. allows individual master_file ocr)
	if b, err := strconv.ParseBool(ocr.req.force); err == nil && b == true {
		ts, tsErr := tsGetPidInfo(ocr.req.pid, ocr.req.unit)

		if tsErr != nil {
			logger.Printf("Tracksys API error: [%s]", tsErr.Error())
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "ERROR: Could not retrieve PID info: [%s]", tsErr.Error())
			return
		}

		ocr.ts = ts

		fmt.Print(w, "OK")

		go generateOcr(ocr)

		return
	}

	// normal request:

	// see if request is already in progress
	if reqInProgress(ocr.workDir) == true {
		// request is in progress; don't start another request, just add email/callback to completion notification list
		logger.Printf("Request already in progress; adding email/callback to completion notification list")
		reqAddEmail(ocr.workDir, ocr.req.email)
		reqAddCallback(ocr.workDir, ocr.req.callback)
		fmt.Print(w, "OK")
		return
	}

	ts, tsErr := tsGetMetadataPidInfo(ocr.req.pid, ocr.req.unit)

	if tsErr != nil {
		logger.Printf("Tracksys API error: [%s]", tsErr.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "ERROR: Could not retrieve PID info: [%s]", tsErr.Error())
		return
	}

	ocr.ts = ts

	/*
		// shouldn't happen from virgo?

		// check if ocr/transcription already exists; if so, just email now

		if ocr.ts.Pid.TextSource != "" {
			logger.Printf("OCR/transcription already exists; emailing now")

			reqAddEmail(ocr.workDir, ocr.req.email)
			reqAddCallback(ocr.workDir, ocr.req.callback)

			res := ocrResultsInfo{}

			res.pid = ocr.req.pid
			res.reqid = ocr.reqID
			res.workDir = ocr.workDir

			for _, p := range ocr.ts.Pages {
				txt, txtErr := tsGetText(p.Pid)
				if txtErr != nil {
					logger.Printf("[%s] tsGetText() error: [%s]", p.Pid, txtErr.Error())
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Print(w, "ERROR: Could not retrieve page text")
					res.details = "Error encountered while retrieving text for one or more pages"
					processOcrFailure(res)
					return
				}

				res.pages = append(res.pages, ocrPidInfo{pid: p.Pid, title: p.Title, text: txt})
			}

			processOcrSuccess(res)

			fmt.Print(w, "OK")
			return
		}
	*/

	// check if this is ocr-able

	if ocr.ts.isOcrable == false {
		logger.Printf("Cannot OCR: [%s]", ocr.ts.Pid.OcrHint)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Print(w, "ERROR: PID is not in a format conducive to OCR")
		return
	}

	// perform ocr

	fmt.Print(w, "OK")

	go generateOcr(ocr)
}

func getTextForMetadataPid(ts *tsPidInfo) (string, error) {
	var ocrText strings.Builder

	// preallocate buffer with assumed worst-case of 4K bytes per page
	ocrText.Grow(len(ts.Pages) * 4096)

	for i, p := range ts.Pages {
		pageText, txtErr := tsGetText(p.Pid)
		if txtErr != nil {
			logger.Printf("[%s] tsGetText() error: [%s]", p.Pid, txtErr.Error())
			return "", errors.New("could not retrieve page text")
		}

		fmt.Fprintf(&ocrText, "[Title: %s]\n[Page: %d of %d]\n\n%s\n\n", p.Title, i+1, len(ts.Pages), pageText)
	}

	return ocrText.String(), nil
}

func ocrTextHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	logger.Printf("%s %s", r.Method, r.RequestURI)

	ocr := ocrInfo{}

	// save fields from original request
	ocr.req.pid = params.ByName("pid")
	ocr.req.unit = r.URL.Query().Get("unit")

	ts, tsErr := tsGetMetadataPidInfo(ocr.req.pid, ocr.req.unit)

	if tsErr != nil {
		logger.Printf("Tracksys API error: [%s]", tsErr.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "ERROR: Could not retrieve PID info: [%s]", tsErr.Error())
		return
	}

	ocrText, txtErr := getTextForMetadataPid(ts)

	if txtErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "ERROR: %s", txtErr.Error())
		return
	}

	fmt.Fprintf(w, "%s", ocrText)
}

func ocrStatusHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	logger.Printf("%s %s", r.Method, r.RequestURI)

	ocr := ocrInfo{}

	// save fields from original request
	ocr.req.pid = params.ByName("pid")
	ocr.req.unit = r.URL.Query().Get("unit")

	ts, tsErr := tsGetMetadataPidInfo(ocr.req.pid, ocr.req.unit)

	if tsErr != nil {
		logger.Printf("Tracksys API error: [%s]", tsErr.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "ERROR: Could not retrieve PID info: [%s]", tsErr.Error())
		return
	}

	hasOcr, hasTranscription := false, false

	switch ts.Pid.TextSource {
	case "ocr", "corrected_ocr":
		hasOcr = true
	case "transcription":
		hasTranscription = true
	}

	status := struct {
		HasOcr           bool `json:"has_ocr"`
		HasTranscription bool `json:"has_transcription"`
		IsOcrCandidate   bool `json:"is_ocr_candidate"`
	}{
		HasOcr:           hasOcr,
		HasTranscription: hasTranscription,
		IsOcrCandidate:   ts.isOcrable,
	}

	output, jsonErr := json.Marshal(status)
	if jsonErr != nil {
		logger.Printf("Failed to serialize output: [%s]", jsonErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "ERROR: Could not serialize PID status: [%s]", jsonErr.Error())
		return
	}

	fmt.Print(w, string(output))
}

func generateOcr(ocr ocrInfo) {
	// check for language override
	if ocr.req.lang != "" {
		ocr.ts.Pid.OcrLanguageHint = ocr.req.lang
	}

	reqInitialize(ocr.workDir, ocr.reqID)
	reqUpdateStarted(ocr.workDir, ocr.reqID)
	reqAddEmail(ocr.workDir, ocr.req.email)
	reqAddCallback(ocr.workDir, ocr.req.callback)

	if err := awsGenerateOcr(ocr); err != nil {
		logger.Printf("generateOcr() failed: [%s]", err.Error())

		res := ocrResultsInfo{}

		res.pid = ocr.req.pid
		res.reqid = ocr.reqID
		res.workDir = ocr.workDir
		res.details = "Error encountered while starting the OCR process"

		processOcrFailure(res)
	}
}
