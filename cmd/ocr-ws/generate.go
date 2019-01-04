package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

type ocrRequest struct {
	pid   string
	unit  string
	pages string
	token string
	email string
	force string
	lang  string
}

type ocrInfo struct {
	req     ocrRequest // values from original request
	ts      *tsPidInfo // values looked up in tracksys
	unitID  int
	subDir  string
	workDir string
	reqID   string
}

/**
 * Handle a request for OCR of page images
 */
func generateHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	logger.Printf("%s %s", r.Method, r.RequestURI)

	ocr := ocrInfo{}

	// save fields from original request
	ocr.req.pid = params.ByName("pid")
	ocr.req.unit = r.URL.Query().Get("unit")
	ocr.req.pages = r.URL.Query().Get("pages")
	ocr.req.token = r.URL.Query().Get("token")
	ocr.req.email = r.URL.Query().Get("email")
	ocr.req.force = r.URL.Query().Get("force")
	ocr.req.lang = r.URL.Query().Get("lang")

	// save info generated from the original request
	ocr.unitID, _ = strconv.Atoi(ocr.req.unit)
	ocr.subDir = ocr.req.pid
	ocr.reqID = newUUID()

	if ocr.unitID > 0 {
		// if pages from a specific unit are requested, put them
		// in a unit subdirectory under the metadata pid
		ocr.subDir = fmt.Sprintf("%s/%d", ocr.req.pid, ocr.unitID)
	}

	if len(ocr.req.pages) > 0 {
		if len(ocr.req.token) == 0 {
			logger.Printf("Request for partial OCR is missing a token")
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "ERROR: Missing token")
			return
		}
		ocr.subDir = ocr.req.token
		logger.Printf("Request for partial OCR including pages: %s", ocr.req.pages)
	}

	// See if destination already exists...
	ocr.workDir = getWorkDir(ocr.subDir)

	if _, err := os.Stat(sqlFileName(ocr.workDir)); err == nil {
		// request database already exists; don't start another request, just add email to requestor list
		logger.Printf("Request already in progress or completed")
		sqlAddEmail(ocr.workDir, ocr.req.email)
		fmt.Fprintf(w, "OK")
		return
	}

	ts, tsErr := tsGetPidInfo(ocr, w)

	if tsErr != nil {
		logger.Printf("Tracksys API error: [%s]", tsErr.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "ERROR: Could not retrieve PID info")
		return
	}

	// filter out pages with empty pids

	var pages []tsAPICommonFields
	allTextExists := true

	for _, p := range ts.Pages {
		if p.Pid == "" {
			logger.Printf("skipping page with missing pid: %v", p)
			continue
		}

		pages = append(pages, p)

		if p.TextSource == "" {
			allTextExists = false
		}
	}

	ts.Pages = pages

	ocr.ts = ts

	// ensure we have something to process

	if len(ocr.ts.Pages) == 0 {
		logger.Printf("No pages found")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "ERROR: No pages found for this PID")
		return
	}

	// create work dir now (required for adding email to requestor list)
	os.MkdirAll(ocr.workDir, 0777)

	// check if forcing ocr

	if b, err := strconv.ParseBool(ocr.req.force); err == nil && b == true {
		if ocr.req.lang != "" {
			ocr.ts.OcrLanguageHint = ocr.req.lang
		}

		sqlAddEmail(ocr.workDir, ocr.req.email)

		fmt.Fprintf(w, "OK")

		go generateOcr(ocr)

		return
	}

	// check if ocr/transcription already exists; if so, just email now

	if allTextExists == true {
		logger.Printf("OCR/transcription already exists; emailing now")

		sqlAddEmail(ocr.workDir, ocr.req.email)

		res := ocrResultsInfo{}

		res.pid = ocr.req.pid
		res.workDir = ocr.workDir

		for _, p := range ocr.ts.Pages {
			txt, txtErr := tsGetText(p.Pid)
			if txtErr != nil {
				logger.Printf("[%s] tsGetText() error: [%s]", p.Pid, txtErr.Error())
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, "ERROR: Could not retrieve page text")
				res.details = "Error encountered while retrieving text for one or more pages"
				processOcrFailure(res)
				return
			}

			res.pages = append(res.pages, ocrPidInfo{pid: p.Pid, title: p.Title, text: txt})
		}

		processOcrSuccess(res)

		fmt.Fprintf(w, "OK")
		return
	}

	// check if this is ocr-able

	if ocr.ts.OcrHint != "" && ocr.ts.OcrHint != "Modern Font" && ocr.ts.OcrHint != "Regular Font" {
		logger.Printf("Cannot OCR: [%s]", ocr.ts.OcrHint)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "ERROR: PID is not in a format conducive to OCR")
		return
	}

	// debug info
	n := len(ocr.ts.Pages)
	logger.Printf("%d pids:", n)
	for i, p := range ocr.ts.Pages {
		txt, txtErr := tsGetText(p.Pid)
		switch {
		case txtErr != nil:
			logger.Printf("[%d/%d] [%s] tsGetText() error: [%s]", i+1, n, p.Pid, txtErr.Error())
		case txt == "":
			logger.Printf("[%d/%d] [%s] no text", i+1, n, p.Pid)
		default:
			//logger.Printf("[%d/%d] [%s] text:\n\n%s\n\n", i+1, n, p.Pid, txt)
		}
	}

	// perform ocr

	sqlAddEmail(ocr.workDir, ocr.req.email)

	fmt.Fprintf(w, "OK")

	go generateOcr(ocr)
}

func generateOcr(ocr ocrInfo) {
	if err := awsGenerateOcr(ocr); err != nil {
		logger.Printf("generateOcr() failed: [%s]", err.Error())

		res := ocrResultsInfo{}

		res.pid = ocr.req.pid
		res.workDir = ocr.workDir
		res.details = "Error encountered while starting the OCR process"

		processOcrFailure(res)
	}
}
