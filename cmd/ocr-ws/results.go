package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/gomail.v2"
)

func sendEmail(m *gomail.Message) {
	d := gomail.Dialer{Host: "smtp.mail.virginia.edu", Port: 25}

	to := m.GetHeader("To")
	subject := m.GetHeader("Subject")

	if err := d.DialAndSend(m); err != nil {
		logger.Printf("Failed to send email to %s: [%s]", to, err.Error())
	} else {
		logger.Printf("Email sent to %s with subject %s", to, subject)
	}
}

func emailSuccess(m *gomail.Message, pid string, ocrFile string) {
	m.SetHeader("Subject", fmt.Sprintf("OCR results for %s", pid))
	m.SetBody("text/plain", "OCR results are attached.")
	m.Attach(ocrFile)

	sendEmail(m)
}

func emailFailure(m *gomail.Message, pid string, errMsg string) {
	m.SetHeader("Subject", fmt.Sprintf("OCR failure for %s", pid))
	m.SetBody("text/plain", fmt.Sprintf("Failure reason: %s", errMsg))

	sendEmail(m)
}

func monitorProgressAndNotifyResults(workDir string, pid string, ocrEmail string) {
	if ocrEmail == "" {
		logger.Printf("Nobody to notify")
		return
	}

	// files to watch for
	outPath := fmt.Sprintf("%s/%s", config.storageDir.value, workDir)
	ocrFile := fmt.Sprintf("%s/%s.txt", outPath, pid)
	failFile := fmt.Sprintf("%s/fail.txt", outPath)

	m := gomail.NewMessage()
	m.SetHeader("From", "ocr-ws@lib.virginia.edu")
	m.SetHeader("To", ocrEmail)

	// monitor for files that indicate completion states,
	// making sure progress is being made by monitoring
	// timestamp of the work directory

	monitorStart := time.Now()
	inactivityThreshold := time.Hour.Seconds()
	retry := 5 * time.Second

	for {
		logger.Printf("Checking for OCR completion...")

		if _, err := os.Stat(ocrFile); err == nil {
			emailSuccess(m, pid, ocrFile)
			return
		}

		if _, err := os.Stat(failFile); err == nil {
			emailFailure(m, pid, "failed")
			return
		}

		dirStat, err := os.Stat(outPath)
		if err != nil {
			logger.Printf("Directory stat error: [%s]", err.Error())

			if os.IsNotExist(err) {
				dirUnavailable := time.Since(monitorStart).Seconds()
				if dirUnavailable > inactivityThreshold {
					emailFailure(m, pid, "timed out")
					return
				}
			} else {
				emailFailure(m, pid, "unknown error")
				return
			}
		} else {
			// if more than a certain amount of time with no change to the directory (i.e. progress), give up
			dirInactivity := time.Since(dirStat.ModTime()).Seconds()

			logger.Printf("Work directory last modified %0.0f seconds ago (considered abandoned at %0.0f)", dirInactivity, inactivityThreshold)

			if dirInactivity > inactivityThreshold {
				emailFailure(m, pid, "stalled")
				return
			}
		}

		time.Sleep(retry)
	}
}
