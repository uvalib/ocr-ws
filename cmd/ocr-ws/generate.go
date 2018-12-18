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
}

type ocrInfo struct {
	req     ocrRequest // values from original request
	ts      *tsPidInfo // values looked up in tracksys
	unitID  int
	subDir  string
	workDir string
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
	ocr.req.email = r.URL.Query().Get("email")
	ocr.req.pages = r.URL.Query().Get("pages")
	ocr.req.token = r.URL.Query().Get("token")

	// save info generated from the original request
	ocr.unitID, _ = strconv.Atoi(ocr.req.unit)
	ocr.subDir = ocr.req.pid

	if ocr.unitID > 0 {
		// if pages from a specific unit are requested, put them
		// in a unit subdirectory under the metadata pid
		ocr.subDir = fmt.Sprintf("%s/%d", ocr.req.pid, ocr.unitID)
	}

	if len(ocr.req.pages) > 0 {
		if len(ocr.req.token) == 0 {
			logger.Printf("Request for partial OCR is missing a token")
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Missing token")
			return
		}
		ocr.subDir = ocr.req.token
		logger.Printf("Request for partial OCR including pages: %s", ocr.req.pages)
	}

	// See if destination already exists...
	ocr.workDir = getWorkDir(ocr.subDir)

	if _, err := os.Stat(ocr.workDir); err == nil {
		// path already exists; don't start another request, just start
		// normal completion polling routine
		logger.Printf("Request already in progress or completed")
		sqlAddEmail(ocr.workDir, ocr.req.email)
		return
	}

	ts, tsErr := tsGetPidInfo(ocr, w)

	if tsErr != nil {
		logger.Printf("Tracksys API error: [%s]", tsErr.Error())
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Tracksys API error: [%s]", tsErr.Error())
		return
	}

	// filter out pages with empty pids

	var pages []tsAPICommonFields

	for _, p := range ts.Pages {
		if p.Pid == "" {
			logger.Printf("skipping page with missing pid: %V", p)
			continue
		}

		pages = append(pages, p)
	}

	ts.Pages = pages

	ocr.ts = ts

	// ensure we have something to process

	if len(ocr.ts.Pages) == 0 {
		logger.Printf("No pages found")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "No pages found for this PID")
		return
	}

	// debug info
	n := len(ocr.ts.Pages)
	logger.Printf("%d pids:", n)
	for i, p := range ocr.ts.Pages {
		txt, txtErr := tsGetText(ocr.req.pid)
		switch {
		case txtErr != nil:
			logger.Printf("[%d/%d] [%s] tsGetText() error: [%s]", i+1, n, p.Pid, txtErr.Error())
		case txt == "":
			logger.Printf("[%d/%d] [%s] no text", i+1, n, p.Pid)
		default:
			logger.Printf("[%d/%d] [%s] text:\n\n%s\n\n", i+1, n, p.Pid, txt)
			//tsPostText(ocr.req.pid, "blah")
		}
	}

	// create work dir
	os.MkdirAll(ocr.workDir, 0777)

	sqlAddEmail(ocr.workDir, ocr.req.email)

	// kick off lengthy OCR generation in a go routine
	go generateOcr(ocr)
}

func generateOcr(ocr ocrInfo) {
	logger.Printf("calling awsGenerateOcr()...")
	awsGenerateOcr(ocr)
}
