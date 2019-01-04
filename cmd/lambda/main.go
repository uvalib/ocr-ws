package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// json for workflow <-> lambda communication
type lambdaRequest struct {
	Lang   string `json:"lang,omitempty"`   // language to use for ocr
	Bucket string `json:"bucket,omitempty"` // s3 bucket for source image
	Key    string `json:"key,omitempty"`    // s3 key for source image
	Pid    string `json:"pid,omitempty"`    // for workflow tracking; unused in lambda
	Title  string `json:"title,omitempty"`  // for workflow tracking; unused in lambda
	Count  int    `json:"count,omitempty"`  // for workflow tracking; unused in lambda
}

type lambdaResponse struct {
	Text string `json:"text,omitempty"`
}

func awsDownloadImage(req lambdaRequest, localFile string) (int64, error) {
	sess, sessErr := session.NewSession()
	if sessErr != nil {
		return -1, errors.New(fmt.Sprintf("Failed to create new session: [%s]", sessErr.Error()))
	}

	downloader := s3manager.NewDownloader(sess)

	f, fileErr := os.Create(localFile)
	if fileErr != nil {
		return -1, errors.New(fmt.Sprintf("Failed to create local file: [%s]", fileErr.Error()))
	}
	defer f.Close()

	bytes, dlErr := downloader.Download(f,
		&s3.GetObjectInput{
			Bucket: aws.String(req.Bucket),
			Key:    aws.String(req.Key),
		})

	if dlErr != nil {
		return -1, errors.New(fmt.Sprintf("Failed to download s3 file: [%s]", dlErr.Error()))
	}

	return bytes, nil
}

func handleOcrRequest(ctx context.Context, req lambdaRequest) (string, error) {
	res := lambdaResponse{}

	imgFile := fmt.Sprintf("/tmp/%s", path.Base(req.Key))

	bytes, dlErr := awsDownloadImage(req, imgFile)
	if dlErr != nil {
		return "", dlErr
	}

	//res.Text = fmt.Sprintf("lang: [%s]  bucket: [%s]  key: [%s]  pid: [%s]  title: [%s]  count: [%d]", req.Lang, req.Bucket, req.Key, req.Pid, req.Title, req.Count)
	res.Text = fmt.Sprintf("imgFile: [%s]  bytes: [%d]\n\nreq:\n\n%v", imgFile, bytes, req)

	output, jsonErr := json.Marshal(res)
	if jsonErr != nil {
		return "", errors.New("Failed to serialize output")
	}

	return string(output), nil
}

func main() {
	lambda.Start(handleOcrRequest)
}
