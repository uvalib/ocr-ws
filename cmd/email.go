package main

import (
	"fmt"
	"log"

	"gopkg.in/gomail.v2"
)

func sendEmail(m *gomail.Message) {
	d := gomail.Dialer{Host: config.emailHost.value, Port: config.emailPort.value}

	to := m.GetHeader("To")
	subject := m.GetHeader("Subject")

	if err := d.DialAndSend(m); err != nil {
		log.Printf("ERROR: failed to send email to %s: [%s]", to, err.Error())
	} else {
		log.Printf("INFO: email sent to %s with subject %s", to, subject)
	}
}

func emailResults(to, subject, body, attachment string) {
	if to == "" {
		log.Printf("WARNING: missing email address")
		return
	}

	m := gomail.NewMessage()

	m.SetHeader("From", fmt.Sprintf("%s <%s>", config.emailName.value, config.emailAddress.value))
	m.SetHeader("To", to)
	m.SetHeader("Subject", subject)
	m.SetBody("text/plain", body)

	if attachment != "" {
		m.Attach(attachment)
	}

	sendEmail(m)
}
