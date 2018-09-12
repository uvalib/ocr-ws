package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
)

func checkTracksysDatabase() bool {
	// check tracksys database connection

	tsPid := "uva-lib:2584807"
	cnt, expectedCount := 0, 1
	tsStatus := true

	logger.Printf("[HEALTH] [Tracksys] checking for PID: [%s]", tsPid)

	qs := "select count(*) as cnt from metadata b where pid=?"
	db.QueryRow(qs, tsPid).Scan(&cnt)

	logger.Printf("[HEALTH] [Tracksys] record count: expected %d, got %d", expectedCount, cnt)

	if cnt != expectedCount {
		logger.Printf("[HEALTH] [Tracksys] ERROR: count mismatch")
		tsStatus = false
	}

	logger.Printf("[HEALTH] [Tracksys] SUCCESS")

	return tsStatus
}

func checkIIIFService() bool {
	// check IIIF server

	iiifPid := "uva-lib:2584807"
	size, expectedSize := 0, 71587
	iiifStatus := true

	logger.Printf("[HEALTH] [IIIF] checking for PID: [%s]", iiifPid)

	url := config.iiifUrlTemplate.value
	url = strings.Replace(url, "$PID", iiifPid, 1)

	timeout := time.Duration(5 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Get(url)
	if err != nil {
		logger.Printf("[HEALTH] [IIIF] ERROR: Get: (%s)", err)
		iiifStatus = false
	} else {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logger.Printf("[HEALTH] [IIIF] ERROR: ReadAll: (%s)", err)
			iiifStatus = false
		} else {
			resp.Body.Close()
			size = len(b)

			logger.Printf("[HEALTH] [IIIF] image size: expected %d, got %d", expectedSize, size)

			if size != expectedSize {
				logger.Printf("[HEALTH] [IIIF] size mismatch")
				iiifStatus = false
			}
		}
	}

	logger.Printf("[HEALTH] [IIIF] SUCCESS")

	return iiifStatus
}

func checkFilesystem() bool {
	// create a test file in a random directory to ensure filesystem exists and is writeable

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	testPath := fmt.Sprintf("%s/%05d-%x", config.storageDir.value, os.Getpid(), r.Int63())

	logger.Printf("[HEALTH] [Filesystem] creating random test directory: [%s]", testPath)

	if mkdirErr := os.Mkdir(testPath, 0777); mkdirErr != nil {
		logger.Printf("[HEALTH] [Filesystem] ERROR: Mkdir: (%s)", mkdirErr)
		return false
	}
	defer os.RemoveAll(testPath)

	logger.Printf("[HEALTH] [Filesystem] creating test file")

	testFile, openErr := os.OpenFile(fmt.Sprintf("%s/test.txt", testPath), os.O_CREATE|os.O_RDWR, 0666)
	if openErr != nil {
		logger.Printf("[HEALTH] [Filesystem] ERROR: OpenFile: (%s)", openErr)
		return false
	}
	defer testFile.Close()

	logger.Printf("[HEALTH] [Filesystem] writing to test file")

	if _, writeErr := testFile.WriteString("test"); writeErr != nil {
		logger.Printf("[HEALTH] [Filesystem] ERROR: WriteString: (%s)", writeErr)
		return false
	}

	logger.Printf("[HEALTH] [Filesystem] SUCCESS")

	return true
}

func healthCheckHandler(rw http.ResponseWriter, req *http.Request, params httprouter.Params) {
	rw.Header().Set("Content-Type", "application/json")

	tsStatus := checkTracksysDatabase()
	iiifStatus := checkIIIFService()
	fsStatus := checkFilesystem()

	out := fmt.Sprintf(`{"alive": %t, "iiif": %t, "tracksys": %t, "storage": %t}`, true, iiifStatus, tsStatus, fsStatus)

	if iiifStatus == false {
		http.Error(rw, out, http.StatusInternalServerError)
	} else {
		fmt.Fprintf(rw, out)
	}
}
