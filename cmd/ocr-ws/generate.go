package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"

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

	// pull params for email notification
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

	// pull query string params; embed and unit
	embedStr := r.URL.Query().Get("embed")
	embed := true
	if len(embedStr) == 0 || embedStr == "0" {
		embed = false
	}

	// See if destination already extsts...
	ocrDestPath := fmt.Sprintf("%s/%s", config.storageDir.value, workDir)
	if _, err := os.Stat(ocrDestPath); err == nil {
		// path already exists; don't start another request, just treat
		// this one is if it was successful and render the ajax page
		if embed {
			fmt.Fprintf(w, "ok")
		} else {
			renderAjaxPage(workDir, pid, w)
		}
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
	if embed {
		fmt.Fprintf(w, "ok")
	} else {
		renderAjaxPage(workDir, pid, w)
	}
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
		return
	}

	// if this is a clone, grab the info for the original
	if origID.Valid {
		qs := `select pid, filename, title from master_files where id = ?`
		err = db.QueryRow(qs, origID.Int64).Scan(&pg.PID, &pg.Filename, &pg.Title)
		if err != nil {
			logger.Printf("Request failed: %s", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Unable to OCR: %s", err.Error())
			return
		}
	}

	pages = append(pages, pg)

	return
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
		return
	}

	// Must have availability set
	if availability.Valid == false && config.allowUnpublished.value == false {
		logger.Printf("%s not found", pid)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "%s not found", pid)
		return
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

		// Filter to only pids requested?
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
			return
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
	return
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

func downloadJpgFromIiif(outPath string, pid string) (jpgFileName string, err error) {
	url := config.iiifUrlTemplate.value
	url = strings.Replace(url, "$PID", pid, 1)

	logger.Printf("Downloading JPG from: %s", url)
	response, err := http.Get(url)
	if err != nil || response.StatusCode != 200 {
		err = errors.New("not found")
		return
	}
	defer response.Body.Close()

	jpgFileName = fmt.Sprintf("%s/%s.jpg", outPath, pid)
	destFile, err := os.Create(jpgFileName)
	if err != nil {
		return
	}
	defer destFile.Close()

	s, err := io.Copy(destFile, response.Body)
	if err != nil {
		return
	}

	logger.Printf(fmt.Sprintf("Successful download size: %d", s))
	return
}

func jpgFromTif(outPath string, pid string, tifFile string) (jpgFileName string, err error) {
	jpgFileName = fmt.Sprintf("%s/%s.jpg", outPath, pid)
	bits := strings.Split(tifFile, "_")
	srcFile := fmt.Sprintf("%s/%s/%s", config.archiveDir.value, bits[0], tifFile)
	logger.Printf("Using archived file as source: %s", srcFile)
	_, err = os.Stat(srcFile)
	if err != nil {
		logger.Printf("ERROR %s", err.Error())
		return
	}

	// run imagemagick to create scaled down jpg
	cmd := "convert"
	args := []string{fmt.Sprintf("%s[0]", srcFile), "-resize", "1024", jpgFileName}
	convErr := exec.Command(cmd, args...).Run()
	if convErr != nil {
		logger.Printf("Unable to generate JPG for %s", tifFile)
	} else {
		logger.Printf("Generated %s", jpgFileName)
	}
	return
}

func updateProgress(outPath string, step int, steps int) {
	logger.Printf("%d%% (step %d of %d)", (100*step)/steps, step, steps)

	f, _ := os.OpenFile(fmt.Sprintf("%s/progress.txt", outPath), os.O_CREATE|os.O_RDWR, 0666)
	defer f.Close()

	w := bufio.NewWriter(f)

	if _, err := fmt.Fprintf(w, "%d%%", (100*step)/steps); err != nil {
		logger.Printf("Unable to write progress file : %s", err.Error())
	}

	w.Flush()
}

/**
 * use jp2 or archived tif files to generate OCR for a PID
 */
func generateOcr(workDir string, pid string, ocrEmail string, pages []pageInfo) {
	// Make sure the work directory exists
	outPath := fmt.Sprintf("%s/%s", config.storageDir.value, workDir)
	os.MkdirAll(outPath, 0777)

	// initialize progress reporting:
	// steps include each page download, each image OCR, plus a final conversion step

	var steps = 2 * len(pages) + 1
	var step = 0

	// iterate over page info and build a list of paths to
	// the image for that page. Older pages may only be stored on lib_content44
	// and newer pages will have a jp2k file avialble on the iiif server
	var jpgFiles []string
	for _, val := range pages {
		logger.Printf("Get reduced size jpg for %s %s", val.PID, val.Filename)

		step++
		updateProgress(outPath, step, steps)

		// First, try to get a JPG file from the IIIF server mount
		jpgFile, jpgErr := downloadJpgFromIiif(outPath, val.PID)
		if jpgErr != nil {
			// not found. work from the archival tif file
			logger.Printf("No JPG found on IIIF server; using archival tif...")
			jpgFile, jpgErr = jpgFromTif(outPath, val.PID, val.Filename)
			if jpgErr != nil {
				logger.Printf("Unable to find source image for masterFile %s. Skipping.", val.PID)
				// account for one fewer ocr to perform
				steps--
				continue
			}
		}
		jpgFiles = append(jpgFiles, jpgFile)
	}

	// check if we have any jpg files to process

	if len(jpgFiles) == 0 {
		logger.Printf("No jpg files to process")
		ef, _ := os.OpenFile(fmt.Sprintf("%s/fail.txt", outPath), os.O_CREATE|os.O_RDWR, 0666)
		defer ef.Close()
		if _, err := ef.WriteString("No jpg files to process"); err != nil {
			logger.Printf("Unable to write error file : %s", err.Error())
		}
		return
	}

	// Perform OCR on each jpg

	var ocrFiles []string
	for _, jpgFile := range jpgFiles {
		step++
		updateProgress(outPath, step, steps)

		ocrFile := fmt.Sprintf("%s/%s/%s.ocr", config.storageDir.value, workDir, pid)
		logger.Printf("Generating page OCR from file: %s into file: %s", jpgFile, ocrFile)
		cmd := "tesseract"
		args := []string{}
		args = append(args, "-l")
		args = append(args, "eng")
		args = append(args, jpgFile)
		args = append(args, ocrFile)
		ocrErr := exec.Command(cmd, args...).Run()
		if ocrErr != nil {
			logger.Printf("Unable to generate OCR : %s", ocrErr.Error())
		} else {
			logger.Printf("Generated OCR : %s", ocrFile)
			// tesseract appends .txt to whatever output file name we specify
			ocrFiles = append(ocrFiles, ocrFile + ".txt")
		}
	}

	// check if we have any ocr files to process

	if len(ocrFiles) == 0 {
		logger.Printf("No ocr files to process")
		ef, _ := os.OpenFile(fmt.Sprintf("%s/fail.txt", outPath), os.O_CREATE|os.O_RDWR, 0666)
		defer ef.Close()
		if _, err := ef.WriteString("No ocr files to process"); err != nil {
			logger.Printf("Unable to write error file : %s", err.Error())
		}
		return
	}

	// Now merge all of the files into 1 text file

	txtFile := fmt.Sprintf("%s/%s/%s.txt", config.storageDir.value, workDir, pid)
	logger.Printf("Merging page OCRs into single txt %s", txtFile)
/*
	cmd := "cat"
	args := []string{}
	args = append(args, ocrFiles...)
	args = append(args, ">")
	args = append(args, txtFile)
*/
	cmd := "sed"
	args := []string{}
	args = append(args, "-n")
	args = append(args, "w" + txtFile)
	args = append(args, ocrFiles...)

	convErr := exec.Command(cmd, args...).Run()
	if convErr != nil {
		logger.Printf("Unable to generate merged txt : %s", convErr.Error())
		ef, _ := os.OpenFile(fmt.Sprintf("%s/fail.txt", outPath), os.O_CREATE|os.O_RDWR, 0666)
		defer ef.Close()
		if _, err := ef.WriteString(convErr.Error()); err != nil {
			logger.Printf("Unable to write error file : %s", err.Error())
		}
	} else {
		logger.Printf("Generated txt : %s", txtFile)
		ef, _ := os.OpenFile(fmt.Sprintf("%s/done.txt", outPath), os.O_CREATE|os.O_RDWR, 0666)
		defer ef.Close()
		if _, err := ef.WriteString(txtFile); err != nil {
			logger.Printf("Unable to write done file : %s", err.Error())
		}
	}

	step++
	updateProgress(outPath, step, steps)

	// Cleanup intermediate jpgFiles
//	exec.Command("rm", jpgFiles...).Run()
}
