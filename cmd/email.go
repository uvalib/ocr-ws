package main

import (
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

func emailResults(to, subject, body, attachment string) {
	if to == "" {
		logger.Printf("missing email address")
		return
	}

	m := gomail.NewMessage()

	m.SetHeader("From", "UVA Library OCR-On-Demand <ocr-ws@lib.virginia.edu>")
	m.SetHeader("To", to)
	m.SetHeader("Subject", subject)
	m.SetBody("text/plain", body)

	if attachment != "" {
		m.Attach(attachment)
	}

	sendEmail(m)
}
