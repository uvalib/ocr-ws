package main

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/julienschmidt/httprouter"
)

/**
 * Handle a request for OCR of page images
 */
func generateHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	logger.Printf("%s %s", r.Method, r.RequestURI)
	pid := params.ByName("pid")
	workDir := pid
	unitID, _ := strconv.Atoi(r.URL.Query().Get("unit"))
	if unitID > 0 {
		// if pages from a specific unit are requested, put them
		// in a unit sibdirectory under the metadata pid
		workDir = fmt.Sprintf("%s/%d", pid, unitID)
	}

	// pull params needed for results notification
	ocrEmail := r.URL.Query().Get("email")

	// pull params for select page OCR generation; pages and token
	ocrPages := r.URL.Query().Get("pages")
	ocrToken := r.URL.Query().Get("token")
	if len(ocrPages) > 0 {
		if len(ocrToken) == 0 {
			logger.Printf("Request for partial OCR is missing a token")
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Missing token")
			return
		}
		workDir = ocrToken
		logger.Printf("Request for partial OCR including pages: %s", ocrPages)
	}

	// See if destination already extsts...
	ocrDestPath := fmt.Sprintf("%s/%s", config.storageDir.value, workDir)

	if _, err := os.Stat(ocrDestPath); err == nil {
		// path already exists; don't start another request, just start
		// normal completion polling routine
		logger.Printf("Request already in progress or completed")
		monitorProgressAndNotifyResults(workDir, pid, ocrEmail)
		return
	}

	// Determine what this pid is. Fail if it can't be determined
	pidType := determinePidType(pid)
	logger.Printf("PID %s is a %s", pid, pidType)
	var pages []pageInfo
	var err error
	if pidType == "metadata" {
		pages, err = getMetadataPages(pid, w, unitID, ocrPages)
		if err != nil {
			return
		}
	} else if pidType == "master_file" {
		pages, err = getMasterFilePages(pid, w)
		if err != nil {
			return
		}
	} else {
		logger.Printf("Unknown PID %s", pid)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "PID %s not found", pid)
		return
	}

	// kick off completion polling process in a go routine
	go monitorProgressAndNotifyResults(workDir, pid, ocrEmail)

	// kick off lengthy OCR generation in a go routine
	go generateOcr(workDir, pid, pages)
}

func txtFromTif(outPath string, pid string, tifFile string) (txtFileFull string, err error) {
	txtFile := fmt.Sprintf("%s.txt", pid)
	txtFileFull = fmt.Sprintf("%s/%s", outPath, txtFile)

	bits := strings.Split(tifFile, "_")
	srcFile := fmt.Sprintf("%s/%s/%s", config.archiveDir.value, bits[0], tifFile)
	logger.Printf("Using archived file as source: [%s]", srcFile)

	_, err = os.Stat(srcFile)
	if err != nil {
		logger.Printf("ERROR %s", err.Error())
		return txtFileFull, err
	}

	// run helper script to generate OCR from the given tif
	cmd := fmt.Sprintf("%s/generateOCR.sh", config.scriptDir.value)
	args := []string{srcFile, outPath, txtFile}

	logger.Printf("Executing command: %s with arguments: %v", cmd, args)

	genErr := exec.Command(cmd, args...).Run()

	if genErr != nil {
		logger.Printf("Unable to generate OCR from tif: [%s]", tifFile)
		return txtFileFull, genErr
	} else {
		logger.Printf("Generated %s", txtFile)
	}

	return txtFileFull, nil
}

func generateOcrPage(outPath string, page *pageInfo) {
	page.txtFile = ""

	txtFile, txtErr := txtFromTif(outPath, page.PID, page.Filename)

	if txtErr != nil {
		logger.Printf("Unable to generate OCR from PID %s; skipping.", page.PID)
	} else {
		page.txtFile = txtFile
	}
}

/**
 * use archived tif files to generate OCR for a PID
 */
func generateOcr(workDir string, pid string, pages []pageInfo) {
	// Make sure the work directory exists
	outPath := fmt.Sprintf("%s/%s", config.storageDir.value, workDir)
	os.MkdirAll(outPath, 0777)

	// generate OCR text filenames for each page
	// process each page in a goroutine

	workers, err := strconv.Atoi(config.workerCount.value)

	switch {
	case err != nil:
		workers = 1
	case workers == 0:
		workers = runtime.NumCPU()
	default:
		workers = 1
	}

	logger.Printf("Worker count set to [%s]; starting %d workers", config.workerCount.value, workers)

	wp := workerpool.New(workers)

	start := time.Now()

	for i, _ := range pages {
		thisPage := &pages[i]
		wp.Submit(func() {
			generateOcrPage(outPath, thisPage)
		})
	}

	logger.Printf("Waiting for pages to complete...")
	wp.StopWait()
	logger.Printf("All pages complete")

	// iterate over page info and build a list of text files containing OCR data
	logger.Printf("Checking for scanned pages...")
	var txtFiles []string
	for _, page := range pages {
		logger.Printf("PID %s has txtFile: [%s]", page.PID, page.txtFile)

		if page.txtFile != "" {
			txtFiles = append(txtFiles, page.txtFile)
		}
	}

	if len(txtFiles) == 0 {
		logger.Printf("No OCR text files to process")
		ef, _ := os.OpenFile(fmt.Sprintf("%s/fail.txt", outPath), os.O_CREATE|os.O_RDWR, 0666)
		defer ef.Close()
		if _, err := ef.WriteString("No text files to process"); err != nil {
			logger.Printf("Unable to write error file : %s", err.Error())
		}
		return
	}

	// Now merge all of the text files into 1 text file

	ocrFile := fmt.Sprintf("%s/%s.txt", outPath, pid)
	logger.Printf("Merging page OCRs into single text file: [%s]", ocrFile)

	// run helper program to merge OCR text files
	cmd := fmt.Sprintf("%s/mergeOCR.sh", config.scriptDir.value)
	args := []string{ocrFile}
	args = append(args, txtFiles...)

	logger.Printf("Executing command: %s", cmd)

	convErr := exec.Command(cmd, args...).Run()
	if convErr != nil {
		logger.Printf("Unable to generate merged txt : %s", convErr.Error())
		ef, _ := os.OpenFile(fmt.Sprintf("%s/fail.txt", outPath), os.O_CREATE|os.O_RDWR, 0666)
		defer ef.Close()
		if _, err := ef.WriteString(convErr.Error()); err != nil {
			logger.Printf("Unable to write error file : %s", err.Error())
		}
	} else {
		logger.Printf("Generated txt : %s", ocrFile)
		ef, _ := os.OpenFile(fmt.Sprintf("%s/done.txt", outPath), os.O_CREATE|os.O_RDWR, 0666)
		defer ef.Close()
		if _, err := ef.WriteString("success"); err != nil {
			logger.Printf("Unable to write done file : %s", err.Error())
		}
	}

	elapsed := time.Since(start).Seconds()

	logger.Printf("%d pages scanned in %0.2f seconds (%0.2f seconds/page)",
		len(txtFiles), elapsed, elapsed/float64(len(txtFiles)))

	// Cleanup intermediate txtFiles
	//	exec.Command("rm", txtFiles...).Run()
}
