# OCR Web Service

This is a web service to generate OCR output from metadata records.
It supports the following endpoints:

* / : returns version information
* /healthcheck : returns a JSON object with details about the health of the service
* /ocr/[PID]/?email=<email> : emails OCR text for the given PID, generating it if necessary

### Notes

* Works in conjunction with the [OCR Lambda Environment](https://github.com/uvalib/ocr-lambda).

### System Requirements

* GO version 1.11.0 or greater
