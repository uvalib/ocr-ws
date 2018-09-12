# OCR Web Service

This is a web service to generate OCR output from metadata records.
It supports the following endpoints:

* / : returns version information
* /healthcheck : returns a JSON object with details about the health of the service
* /ocr/[PID] : downloads OCR text for the given PID, generating it if necessary
* /ocr/[PID]/status : displays the OCR generation status of the given PID (e.g. nonexistent, progress percentage, failed, complete)
* /ocr/[PID]/download : downloads OCR text for the given PID (does not generate it if it does not exist)
* /ocr/[PID]/delete : removes cached OCR text (can be used to reclaim space, or to support regeneration of OCR text)

### System Requirements

* GO version 1.9.2 or greater
* DEP (https://golang.github.io/dep/) version 0.4.1 or greater
