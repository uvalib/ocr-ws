package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gammazero/workerpool"
)

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

func localGenerateOcrPage(outPath string, page *pageInfo) {
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
func localGenerateOcr(ocr ocrInfo) {
	// kick off completion polling process in a go routine
	go monitorProgressAndNotifyResults(ocr.workDir, ocr.req.pid, ocr.req.email)

	// Make sure the work directory exists
	outPath := fmt.Sprintf("%s/%s", config.storageDir.value, ocr.workDir)
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

	for i, _ := range ocr.pages {
		thisPage := &ocr.pages[i]
		wp.Submit(func() {
			localGenerateOcrPage(outPath, thisPage)
		})
	}

	logger.Printf("Waiting for pages to complete...")
	wp.StopWait()
	logger.Printf("All pages complete")

	// iterate over page info and build a list of text files containing OCR data
	logger.Printf("Checking for scanned pages...")
	var txtFiles []string
	for _, page := range ocr.pages {
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

	ocrFile := fmt.Sprintf("%s/%s.txt", outPath, ocr.req.pid)
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
