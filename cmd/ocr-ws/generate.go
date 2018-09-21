package main

import (
	"database/sql"
	"errors"
	"fmt"
	"html/template"
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

func determinePidType(pid string) (pidType string) {
	var cnt int

	pidType = "invalid"

	qs := "select count(*) as cnt from metadata b where pid=?"
	db.QueryRow(qs, pid).Scan(&cnt)

	if cnt == 1 {
		pidType = "metadata"
		return
	}

	qs = "select count(*) as cnt from master_files b where pid=?"
	db.QueryRow(qs, pid).Scan(&cnt)

	if cnt == 1 {
		pidType = "master_file"
		return
	}

	return
}

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

	// pull param for email notification
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
		// path already exists; don't start another request, just treat
		// this one is if it was successful and render the ajax page
		logger.Printf("Request already in progress or completed")
		renderAjaxPage(workDir, pid, w)
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

	// kick the lengthy OCR generation off in a go routine
	go generateOcr(workDir, pid, ocrEmail, pages)

	// Render a simple ok message or kick an ajax polling loop
	renderAjaxPage(workDir, pid, w)
}

func getMasterFilePages(pid string, w http.ResponseWriter) (pages []pageInfo, err error) {
	var pg pageInfo
	var origID sql.NullInt64
	qs := `select pid, filename, title, original_mf_id from master_files where pid = ?`
	err = db.QueryRow(qs, pid).Scan(&pg.PID, &pg.Filename, &pg.Title, &origID)
	if err != nil {
		logger.Printf("Request failed: %s", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Unable to OCR: %s", err.Error())
		return nil, err
	}

	// if this is a clone, grab the info for the original
	if origID.Valid {
		qs := `select pid, filename, title from master_files where id = ?`
		err = db.QueryRow(qs, origID.Int64).Scan(&pg.PID, &pg.Filename, &pg.Title)
		if err != nil {
			logger.Printf("Request failed: %s", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Unable to OCR: %s", err.Error())
			return nil, err
		}
	}

	pages = append(pages, pg)

	return pages, nil
}

// NOTE: when called from Tracksys, the unitID will be set. Honor this and generate OCR
// of all masterfiles in that unit regardless of published status. When called from Virgo,
// unitID will NOT be set. Run through all units and only include those that are
// in the DL and are publicly visible
//
func getMetadataPages(pid string, w http.ResponseWriter, unitID int, ocrPages string) (pages []pageInfo, err error) {
	// Get metadata for the passed PID
	logger.Printf("Get Metadata pages params: PID: %s, Unit %d, Pages: %s", pid, unitID, ocrPages)

	var availability sql.NullInt64
	var metadataID int
	var title string
	qs := "select id, title, availability_policy_id from metadata where pid=?"
	err = db.QueryRow(qs, pid).Scan(&metadataID, &title, &availability)
	if err != nil {
		logger.Printf("Request failed: %s", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Unable to OCR: %s", err.Error())
		return nil, err
	}

	// Must have availability set
	if availability.Valid == false && config.allowUnpublished.value == false {
		logger.Printf("%s not found", pid)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "%s not found", pid)
		return nil, errors.New("PID does not have availability set")
	}

	// Get data for all master files from units associated with metadata / unit
	// Do this in two passes, once for orignal master files and once for clones
	for i := 0; i < 2; i++ {
		queryParams := []interface{}{metadataID}
		if i == 0 {
			// Non-cloned master files
			qs = `select m.id, m.pid, m.filename, m.title from master_files m
               inner join units u on u.id = m.unit_id
               where m.metadata_id = ? and u.include_in_dl = 1 and m.original_mf_id is null`
			if unitID > 0 {
				qs = `select m.id, m.pid, m.filename, m.title from master_files m
                  where unit_id = ? and m.original_mf_id is null`
				queryParams = []interface{}{unitID}
			}
		} else {
			// Cloned master files
			qs = `select om.id, om.pid, om.filename, om.title from master_files m
			      inner join master_files om on om.id = m.original_mf_id
               inner join units u on u.id = m.unit_id
			      where m.metadata_id = ? and u.include_in_dl = 1 and m.original_mf_id is not null`
			if unitID > 0 {
				qs = `select om.id, om.pid, om.filename, om.title from master_files m
   			      inner join master_files om on om.id = m.original_mf_id
   			      where m.unit_id = ? and m.original_mf_id is not null`
				queryParams = []interface{}{unitID}
			}
		}

		// Filter to only pages requested?
		if len(ocrPages) > 0 {
			idStr := strings.Split(ocrPages, ",")
			for _, val := range idStr {
				id, invalid := strconv.Atoi(val)
				if invalid == nil {
					queryParams = append(queryParams, id)
				}
			}
			qs = qs + " and m.id in (?" + strings.Repeat(",?", len(idStr)-1) + ")"
		}

		logger.Printf("Query: %s, Params: %s", qs, queryParams)
		rows, queryErr := db.Query(qs, queryParams...)
		defer rows.Close()
		if queryErr != nil {
			logger.Printf("Request failed: %s", queryErr.Error())
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Unable to OCR: %s", queryErr.Error())
			return nil, err
		}

		for rows.Next() {
			var pg pageInfo
			var mfID int
			err = rows.Scan(&mfID, &pg.PID, &pg.Filename, &pg.Title)
			if err != nil {
				logger.Printf("Unable to retrieve MasterFile data for OCR generation %s: %s", pid, err.Error())
				continue
			}

			pages = append(pages, pg)
		}
	}
	return pages, nil
}

/*
 * Render a simple html page that will poll for status of this OCR request, and download (email?) it when done
 */
func renderAjaxPage(workDir string, pid string, w http.ResponseWriter) {
	varmap := map[string]interface{}{
		"pid":   pid,
		"token": workDir,
	}
	index := fmt.Sprintf("%s/index.html", config.templateDir.value)
	tmpl, _ := template.ParseFiles(index)
	err := tmpl.ExecuteTemplate(w, "index.html", varmap)
	if err != nil {
		logger.Printf("Unable to render ajax polling page for %s: %s", pid, err.Error())
		fmt.Fprintf(w, "Unable to render ajax polling page for %s: %s", pid, err.Error())
	}
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
func generateOcr(workDir string, pid string, ocrEmail string, pages []pageInfo) {
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

	ocrFile := fmt.Sprintf("%s/%s/%s.txt", config.storageDir.value, workDir, pid)
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
