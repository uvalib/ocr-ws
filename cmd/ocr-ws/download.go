package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/julienschmidt/httprouter"
)

func downloadHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
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
	f, err := os.OpenFile(fmt.Sprintf("%s/done.txt", workDir), os.O_RDONLY, 0666)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Not found")
		return
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Unable to find OCR for this PID")
		return
	}
	logger.Printf("Sending %s to client", b)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s.txt", pid))
	w.Header().Set("Content-Type", "text/plain")
	in, err := os.Open(string(b))
	if err != nil {
		return
	}
	defer in.Close()
	io.Copy(w, in)
	logger.Printf("OCR for %s completed successfully", pid)
}
