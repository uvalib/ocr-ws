package main

import (
//	"fmt"
//	"io/ioutil"
//	"math/rand"
//	"net/http"
//	"os"
//	"strings"
//	"time"

//	"github.com/aws/aws-sdk-go/aws"
//	"github.com/aws/aws-sdk-go/aws/awserr"
//	"github.com/aws/aws-sdk-go/aws/request"
//	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/swf"
)

func awsPollForDecisions() {
	svc := swf.New(sess)

	for {
		logger.Printf("polling for decision...")

		pageNum := 0
		params := (&swf.PollForDecisionTaskInput{}).
			SetDomain("OcrWorkflowStaging").
			SetTaskList((&swf.TaskList{}).SetName("OcrMainTaskList"))

		err := svc.PollForDecisionTaskPages(params,
			func(page *swf.PollForDecisionTaskOutput, lastPage bool) bool {
				pageNum++
				logger.Printf("[page %d]\n%s", pageNum, page)
				return true
			})

		if err != nil {
			logger.Printf("polling err: %s", err.Error())
		}
	}
}
