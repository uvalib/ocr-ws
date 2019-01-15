package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/julienschmidt/httprouter"
)

func checkFilesystem() bool {
	// create a test file in a random directory to ensure filesystem exists and is writeable

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	testPath := fmt.Sprintf("%s/%05d-%x", config.storageDir.value, os.Getpid(), r.Int63())

	logger.Printf("[HEALTH] [Filesystem] creating random test directory: [%s]", testPath)

	if mkdirErr := os.Mkdir(testPath, 0775); mkdirErr != nil {
		logger.Printf("[HEALTH] [Filesystem] ERROR: Mkdir: (%s)", mkdirErr)
		return false
	}
	defer os.RemoveAll(testPath)

	logger.Printf("[HEALTH] [Filesystem] creating test file")

	testFile, openErr := os.OpenFile(fmt.Sprintf("%s/test.txt", testPath), os.O_CREATE|os.O_RDWR, 0664)
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

	//tsStatus := checkTracksysAPI()
	tsStatus := true
	fsStatus := checkFilesystem()

	out := fmt.Sprintf(`{"alive": %t, "tracksys": %t, "storage": %t}`, true, tsStatus, fsStatus)

	fmt.Fprintf(rw, out)
}
