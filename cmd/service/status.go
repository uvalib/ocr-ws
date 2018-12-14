package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/julienschmidt/httprouter"
)

func statusHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	logger.Printf("%s %s", r.Method, r.RequestURI)
	pid := params.ByName("pid")
	token := r.URL.Query().Get("token")
	workDir := fmt.Sprintf("%s/%s", config.storageDir.value, pid)

	if len(token) > 0 {
		workDir = fmt.Sprintf("%s/%s", config.storageDir.value, token)
	}

	if _, err := os.Stat(workDir); err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Not found")
		return
	}

	if _, err := os.Stat(fmt.Sprintf("%s/done.txt", workDir)); err == nil {
		fmt.Fprintf(w, "READY")
		return
	}

	errorFile := fmt.Sprintf("%s/fail.txt", workDir)
	if _, err := os.Stat(errorFile); err == nil {
		fmt.Fprintf(w, "FAILED")
		os.Remove(errorFile)
		os.Remove(workDir)
		return
	}

	progressFile := fmt.Sprintf("%s/progress.txt", workDir)
	prog, err := ioutil.ReadFile(progressFile)
	if err != nil {
		fmt.Fprintf(w, "PROCESSING")
		return
	}

	fmt.Fprintf(w, "%s", string(prog))
}
