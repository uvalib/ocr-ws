package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/swf"
	"github.com/gammazero/workerpool"
)

// holds information we extract from a decision task
type decisionInfo struct {
	input        string
	req          workflowRequest
	taskToken    string
	workflowId   string
	lastEventId  int64
	allEvents    []*swf.HistoryEvent
	recentEvents []*swf.HistoryEvent
	ocrResults   []*swf.HistoryEvent
}

// json for webservice <-> workflow communication
type ocrPageInfo struct {
	Pid      string `json:"p,omitempty"`
	Filename string `json:"f,omitempty"`
}

type workflowRequest struct {
	Pid    string        `json:"pid,omitempty"`
	Path   string        `json:"path,omitempty"`
	Lang   string        `json:"lang,omitempty"`
	ReqID  string        `json:"reqid,omitempty"`
	Bucket string        `json:"bucket,omitempty"`
	Pages  []ocrPageInfo `json:"pages,omitempty"`
}

// json for workflow <-> lambda communication
type lambdaRequest struct {
	Lang      string `json:"lang,omitempty"`      // language to use for ocr
	Bucket    string `json:"bucket,omitempty"`    // s3 bucket for source image
	Key       string `json:"key,omitempty"`       // s3 key for source image
	ParentPid string `json:"parentpid,omitempty"` // pid of metadata parent, if applicable
	Pid       string `json:"pid,omitempty"`       // pid of this master_file image
}

type lambdaResponse struct {
	Text string `json:"text,omitempty"`
}

// json for failed lambda error details
type lambdaFailureDetails struct {
	ErrorMessage string   `json:"errorMessage,omitempty"`
	ErrorType    string   `json:"errorType,omitempty"`
	StackTrace   []string `json:"stackTrace,omitempty"`
}

// globals

var random *rand.Rand

// functions

func awsCompleteWorkflowExecution(result string) *swf.Decision {
	decision := (&swf.Decision{}).
		SetDecisionType("CompleteWorkflowExecution").
		SetCompleteWorkflowExecutionDecisionAttributes((&swf.CompleteWorkflowExecutionDecisionAttributes{}).
			SetResult(result))

	return decision
}

func awsFailWorkflowExecution(reason, details string) *swf.Decision {
	decision := (&swf.Decision{}).
		SetDecisionType("FailWorkflowExecution").
		SetFailWorkflowExecutionDecisionAttributes((&swf.FailWorkflowExecutionDecisionAttributes{}).
			SetReason(reason).
			SetDetails(details))

	return decision
}

func awsScheduleLambdaFunction(input, control string) *swf.Decision {
	decision := (&swf.Decision{}).
		SetDecisionType("ScheduleLambdaFunction").
		SetScheduleLambdaFunctionDecisionAttributes((&swf.ScheduleLambdaFunctionDecisionAttributes{}).
			SetControl(control).
			SetName(config.awsLambdaFunction.value).
			SetId(newUUID()).
			SetInput(input))

	return decision
}

func awsStartTimer(duration int, control string) *swf.Decision {
	decision := (&swf.Decision{}).
		SetDecisionType("StartTimer").
		SetStartTimerDecisionAttributes((&swf.StartTimerDecisionAttributes{}).
			SetControl(control).
			SetStartToFireTimeout(strconv.Itoa(duration)).
			SetTimerId(newUUID()))

	return decision
}

func awsEventWithId(events []*swf.HistoryEvent, eventId int64) *swf.HistoryEvent {
	// event n seems to always be at index n-1 in the event history, but
	// in the absence of documentation of this, we check the list to be safe

	for _, e := range events {
		if eventId == *e.EventId {
			return e
		}
	}

	return nil
}

func awsEventWithType(events []*swf.HistoryEvent, eventType string) *swf.HistoryEvent {
	for _, e := range events {
		if eventType == *e.EventType {
			return e
		}
	}

	return nil
}

func awsFinalizeSuccess(info decisionInfo) {
	res := ocrResultsInfo{}

	res.pid = info.req.Pid
	res.workDir = getWorkDir(info.req.Path)

	for _, e := range info.ocrResults {
		// lambda result is json embedded within a json string value; must unmarshal twice
		a := e.LambdaFunctionCompletedEventAttributes

		var input string

		if jErr := json.Unmarshal([]byte(*a.Result), &input); jErr != nil {
			logger.Printf("Unmarshal() failed [finalize response intermediate]: %s", jErr.Error())
			continue
		}

		lambdaRes := lambdaResponse{}

		if jErr := json.Unmarshal([]byte(input), &lambdaRes); jErr != nil {
			logger.Printf("Unmarshal() failed [finalize response final]: %s", jErr.Error())
			continue
		}

		// get pid from original request
		o := awsEventWithId(info.allEvents, *a.ScheduledEventId)
		origLambdaInput := *o.LambdaFunctionScheduledEventAttributes.Input

		lambdaReq := lambdaRequest{}

		if jErr := json.Unmarshal([]byte(origLambdaInput), &lambdaReq); jErr != nil {
			logger.Printf("Unmarshal() failed [finalize request]: %s", jErr.Error())
			continue
		}

		res.pages = append(res.pages, ocrPidInfo{pid: lambdaReq.Pid, text: lambdaRes.Text})
	}

	// sort by pid
	sort.Slice(res.pages, func(i, j int) bool { return res.pages[i].pid < res.pages[j].pid })

	go processOcrSuccess(res)

	awsDeleteImages(info.req.ReqID)
}

func awsFinalizeFailure(info decisionInfo, details string) {
	res := ocrResultsInfo{}

	res.pid = info.req.Pid
	res.details = details
	res.workDir = getWorkDir(info.req.Path)

	go processOcrFailure(res)

	awsDeleteImages(info.req.ReqID)
}

func awsHandleDecisionTask(svc *swf.SWF, info decisionInfo) {
	workflowHalted := false

	for _, e := range info.allEvents {
		t := *e.EventType

		// extract the original input string containing pids that were processed
		if info.input == "" && t == "WorkflowExecutionStarted" {
			info.input = *e.WorkflowExecutionStartedEventAttributes.Input
			json.Unmarshal([]byte(info.input), &info.req)
			pages := []ocrPageInfo{}
			for _, p := range info.req.Pages {
				if p.Pid != "" {
					pages = append(pages, p)
				}
			}
			info.req.Pages = pages
			//logger.Printf("[%s] input = [%s] (%d pids)", info.workflowId, info.input, len(info.req.Pages))
			logger.Printf("[%s] reqid: [%s]  pages: %d", info.workflowId, info.req.ReqID, len(info.req.Pages))
		}

		// collect the completed (successful) OCR events, which contain the OCR results
		if t == "LambdaFunctionCompleted" {
			//logger.Printf("[%s] lambda completed", info.workflowId)
			info.ocrResults = append(info.ocrResults, e)
		}

		// set a flag if any workflow execution event failed (start, complete, fail)
		if strings.Contains(t, "WorkflowExecutionFailed") {
			workflowHalted = true
		}

		// from here on out, only consider recent events (events that occurred since
		// since the last time a decision task for this workflow was processed)
		if *e.EventId <= info.lastEventId {
			continue
		}

		// collect all non-decision recent events
		if strings.HasPrefix(t, "Decision") {
			continue
		}

		info.recentEvents = append(info.recentEvents, e)
	}

	if workflowHalted {
		logger.Printf("[%s] WORKFLOW WAS PREVIOUSLY HALTED", info.workflowId)
	}

	logger.Printf("[%s] lambdas completed: %d / %d", info.workflowId, len(info.ocrResults), len(info.req.Pages))

	recentCounts := make(map[string]int)
	var lastEventType string
	for _, e := range info.recentEvents {
		recentCounts[*e.EventType]++
		lastEventType = *e.EventType
	}
	logger.Printf("[%s] recent events: %s", info.workflowId, countsToString(recentCounts))
	logger.Printf("[%s] last event type: [%s]", info.workflowId, lastEventType)

	// we can now make decisions about the workflow:
	// if the most recent event type is workflow execution start, kick off lambdas.
	// otherwise, check if we have all completed lambdas; if not, rerun recently
	// failed/timed out lambdas.

	var decisions []*swf.Decision

	switch {
	// completion condition (failure): no pids found in the input string
	// decision: fail the workflow
	case len(info.req.Pages) == 0:
		decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "No PIDs to process"))
		awsFinalizeFailure(info, "No PIDs to process")

	// start of workflow
	// decision(s): schedule a lambda for each pid.  if no pids, fail the workflow
	//case lastEventType == "WorkflowExecutionStarted":
	case recentCounts["WorkflowExecutionStarted"] > 0:
		//logger.Printf("[%s] input = [%s] (%d pids)", info.workflowId, info.input, len(info.req.Pages))
		logger.Printf("[%s] scheduling %d lambdas...", info.workflowId, len(info.req.Pages))

		for _, page := range info.req.Pages {
			req := lambdaRequest{}

			req.Lang = info.req.Lang
			req.Bucket = info.req.Bucket
			req.Key = getS3Filename(info.req.ReqID, page.Filename)
			req.ParentPid = info.req.Pid
			req.Pid = page.Pid

			input, jsonErr := json.Marshal(req)
			if jsonErr != nil {
				logger.Printf("[%s] JSON marshal failed: [%s]", info.workflowId, jsonErr.Error())
				decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "lambda creation failed"))
				awsFinalizeFailure(info, "OCR generation process failed (initialization failed)")
				break
			}

			//logger.Printf("[%s] lambda json: [%s]", info.workflowId, input)

			decisions = append(decisions, awsScheduleLambdaFunction(string(input), "1"))
		}

	// completion condition (success): number of successful lambda executions = number of pids
	// decision: complete the workflow
	case len(info.ocrResults) == len(info.req.Pages):
		// did a previous completion attempt fail?  try, try again
		if e := awsEventWithType(info.recentEvents, "CompleteWorkflowExecutionFailed"); e != nil {
			a := e.CompleteWorkflowExecutionFailedEventAttributes
			logger.Printf("[%s] complete workflow execution failed (%s)", info.workflowId, *a.Cause)
			decisions = append([]*swf.Decision{}, awsCompleteWorkflowExecution("SUCCESS"))
		} else {
			decisions = append([]*swf.Decision{}, awsCompleteWorkflowExecution("success"))
			awsFinalizeSuccess(info)
		}

	// middle of the workflow -- typically this occurs when lambdas complete/fail/timeout
	// decision(s): if lambdas recently failed/timed out, schedule them to be rerun; otherwise
	// send an empty decision (waits for another event to prompt a new decision task)
	default:
		var uniqueEventTypes []string
		for _, e := range info.recentEvents {
			uniqueEventTypes = appendStringIfMissing(uniqueEventTypes, *e.EventType)
		}
		logger.Printf("[%s] unique recent events: [%s]", info.workflowId, strings.Join(uniqueEventTypes, ", "))

	EventsProcessingLoop:
		for _, e := range info.recentEvents {
			t := *e.EventType

			var origLambdaEvent int64
			var origLambdaInput string
			var origLambdaCount string

			// attempt to start the workflow failed: ???
			// decision(s): ???
			if t == "WorkflowExecutionFailed" {
				a := e.WorkflowExecutionFailedEventAttributes
				logger.Printf("[%s] start workflow execution failed (%s) - (%s)", info.workflowId, *a.Reason, *a.Details)
				decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "workflow execution failed"))
				awsFinalizeFailure(info, "OCR generation process failed (could not start process)")
				break EventsProcessingLoop
			}

/*
			// HANDLED ABOVE in the successful completion condition, as we will never reach this point if this occurs

			// attempt to complete the workflow failed: ???
			// decision(s): ???
			if t == "CompleteWorkflowExecutionFailed" {
				a := e.CompleteWorkflowExecutionFailedEventAttributes
				logger.Printf("[%s] complete workflow execution failed (%s)", info.workflowId, *a.Cause)
				decisions = append([]*swf.Decision{}, awsCompleteWorkflowExecution("SUCCESS"))
				break EventsProcessingLoop
			}
*/

			// attempt to fail the workflow failed: ???
			// decision(s): ???
			if t == "FailWorkflowExecutionFailed" {
				a := e.FailWorkflowExecutionFailedEventAttributes
				logger.Printf("[%s] fail workflow execution failed (%s)", info.workflowId, *a.Cause)
				decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("FAILURE", "fail workflow execution failed"))
				break EventsProcessingLoop
			}

			// attempt to start a timer failed: ???
			// decision(s): ???
			if t == "StartTimerFailed" {
				a := e.StartTimerFailedEventAttributes
				logger.Printf("[%s] start timer failed (%s)", info.workflowId, *a.Cause)
			}

			// if this a recently failed lambda execution, determine what to do with it
			if t == "LambdaFunctionFailed" {
				a := e.LambdaFunctionFailedEventAttributes
				reason := *a.Reason

				details := lambdaFailureDetails{}
				json.Unmarshal([]byte(*a.Details), &details)

				if details.ErrorType != "" || details.ErrorMessage != "" {
					logger.Printf("[%s] lambda failed: (%s) : [%s] / [%s]", info.workflowId, reason, details.ErrorType, details.ErrorMessage)
				} else {
					logger.Printf("[%s] lambda failed: (%s)", info.workflowId, reason)
				}

				o := awsEventWithId(info.allEvents, *a.ScheduledEventId)
				origLambdaCount = *o.LambdaFunctionScheduledEventAttributes.Control
				origLambdaEvent = *a.ScheduledEventId
			}

			// if this a recently timed out lambda execution, rerun it
			if t == "LambdaFunctionTimedOut" {
				a := e.LambdaFunctionTimedOutEventAttributes

				logger.Printf("[%s] lambda timed out (%s)", info.workflowId, *a.TimeoutType)

				o := awsEventWithId(info.allEvents, *a.ScheduledEventId)
				origLambdaCount = *o.LambdaFunctionScheduledEventAttributes.Control
				origLambdaEvent = *a.ScheduledEventId
			}

			// if this a timer that fired, rerun the associated lambda
			if t == "TimerFired" {
				a := e.TimerFiredEventAttributes

				logger.Printf("[%s] timer fired", info.workflowId)

				o := awsEventWithId(info.allEvents, *a.StartedEventId)

				id, _ := strconv.Atoi(*o.TimerStartedEventAttributes.Control)
				o = awsEventWithId(info.allEvents, int64(id))

				origLambdaCount = *o.LambdaFunctionScheduledEventAttributes.Control
				origLambdaInput = *o.LambdaFunctionScheduledEventAttributes.Input
				origLambdaEvent = int64(id)
			}

			// handle lambda retry-related scenarios:
			// if just count/event is set, start a new timer to delay lambda retry.  if input is also set, rerun the lambda.
			if origLambdaCount != "" && origLambdaEvent != 0 {
				count, _ := strconv.Atoi(origLambdaCount)

				if origLambdaInput != "" {
					// rerun the referenced lambda

					count++

					logger.Printf("[%s] retrying lambda event %d (attempt %d)", info.workflowId, origLambdaEvent, count)

					decisions = append(decisions, awsScheduleLambdaFunction(origLambdaInput, strconv.Itoa(count)))
				} else {
					// start a timer referencing the original lambda to be rerun, with exponential backoff based on execution count

					maxAttempts, _ := strconv.Atoi(config.lambdaAttempts.value)
					if maxAttempts < 1 {
						maxAttempts = 1
					}

					// limit number of reruns
					if count >= maxAttempts {
						logger.Printf("[%s] maximum lambda attempts reached (%d); failing", info.workflowId, maxAttempts)
						decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "maximum OCR attempts reached for one or more pages"))
						awsFinalizeFailure(info, "OCR generation process failed (maximum attempts reached)")
						break EventsProcessingLoop
					}

					delay := int(math.Pow(2, float64(count))) + random.Intn(15)

					logger.Printf("[%s] scheduling lambda event %d to be retried in %d seconds...", info.workflowId, origLambdaEvent, delay)

					decisions = append(decisions, awsStartTimer(delay, fmt.Sprintf("%d", origLambdaEvent)))
				}
			}
		}
	}

	// quick check to ensure all decisions made appear valid

	decisionCounts := make(map[string]int)
	for _, d := range decisions {
		decisionCounts[*d.DecisionType]++

		if err := d.Validate(); err != nil {
			logger.Printf("[%s] decision validation error: [%s]", info.workflowId, err.Error())
			return
		}
	}
	logger.Printf("[%s] decision(s): %s", info.workflowId, countsToString(decisionCounts))

	// build, validate, and send response

	respParams := (&swf.RespondDecisionTaskCompletedInput{}).
		SetDecisions(decisions).
		SetTaskToken(info.taskToken)

	if err := respParams.Validate(); err != nil {
		logger.Printf("[%s] respond validation error: [%s]", info.workflowId, err.Error())
		return
	}

	_, respErr := svc.RespondDecisionTaskCompleted(respParams)

	if respErr != nil {
		logger.Printf("[%s] respond error: [%s]", info.workflowId, respErr.Error())
		return
	}
}

func awsPollForDecisionTasks() {
	svc := swf.New(sess)

	random = rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		var info decisionInfo

		logger.Printf("polling for decision task...")

		pollParams := (&swf.PollForDecisionTaskInput{}).
			SetDomain(config.awsSwfDomain.value).
			SetTaskList((&swf.TaskList{}).
				SetName(config.awsSwfTaskList.value))

		// iterate over pages, collecting initial workflow information to process later
		pollErr := svc.PollForDecisionTaskPages(pollParams,
			func(page *swf.PollForDecisionTaskOutput, lastPage bool) bool {
				if page.PreviousStartedEventId != nil {
					info.lastEventId = *page.PreviousStartedEventId
				}

				if info.taskToken == "" && page.TaskToken != nil {
					info.taskToken = *page.TaskToken
					//logger.Printf("TaskToken  = [%s]", info.taskToken)
				}

				if info.workflowId == "" && page.WorkflowExecution != nil {
					info.workflowId = *page.WorkflowExecution.WorkflowId
					logger.Printf("[%s] <-- working decision from this workflow", info.workflowId)
				}

				info.allEvents = append(info.allEvents, page.Events...)

				return true
			})

		if pollErr != nil {
			logger.Printf("polling error: %s", pollErr.Error())
			time.Sleep(60 * time.Second)
			continue
		}

		if info.taskToken == "" {
			logger.Printf("no decision tasks available")
			continue
		}

		// process this decision task
		awsHandleDecisionTask(svc, info)
	}
}

func awsSubmitWorkflow(req workflowRequest) error {
	svc := swf.New(sess)

	id := newUUID()

	input, jsonErr := json.Marshal(req)
	if jsonErr != nil {
		logger.Printf("JSON marshal failed: [%s]", jsonErr.Error())
		return errors.New("Failed to encode workflow request")
	}

	startParams := (&swf.StartWorkflowExecutionInput{}).
		SetDomain(config.awsSwfDomain.value).
		SetWorkflowId(id).
		SetWorkflowType((&swf.WorkflowType{}).
			SetName(config.awsSwfWorkflowType.value).
			SetVersion(config.awsSwfWorkflowVersion.value)).
		SetTaskList((&swf.TaskList{}).
			SetName(config.awsSwfTaskList.value)).
		SetChildPolicy("TERMINATE").
		SetExecutionStartToCloseTimeout(config.awsSwfWorkflowTimeout.value).
		SetTaskStartToCloseTimeout(config.awsSwfDecisionTimeout.value).
		SetInput(string(input))

	res, startErr := svc.StartWorkflowExecution(startParams)

	if startErr != nil {
		logger.Printf("start error: [%s]", startErr.Error())
		return errors.New("Failed to start OCR workflow")
	}

	logger.Printf("started WorkflowId [%s] with RunId: [%s]", id, *res.RunId)

	return nil
}

func awsDeleteImages(reqDir string) error {
	svc := s3.New(sess)

	logger.Printf("deleting: [%s]", reqDir)

	iter := s3manager.NewDeleteListIterator(svc, &s3.ListObjectsInput{
		Bucket: aws.String(config.awsBucketName.value),
		Prefix: aws.String(fmt.Sprintf("requests/%s", reqDir)),
	})

	err := s3manager.NewBatchDeleteWithClient(svc).Delete(aws.BackgroundContext(), iter)

	if err != nil {
		logger.Printf("S3 delete failed: [%s]", err.Error())
	}

	return err
}

func awsUploadImage(uploader *s3manager.Uploader, reqID, imgFile string) error {
	localFile := getLocalFilename(imgFile)
	s3File := getS3Filename(reqID, imgFile)

	f, err := os.Open(localFile)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to open image file: [%s]", err.Error()))
	}
	defer f.Close()

	logger.Printf("uploading: [%s] => [%s]", localFile, s3File)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(config.awsBucketName.value),
		Key:    aws.String(s3File),
		Body:   f,
	})

	return err
}

func awsUploadImages(ocr ocrInfo) error {
	uploader := s3manager.NewUploader(sess)

	for _, page := range ocr.ts.Pages {
		if err := awsUploadImage(uploader, ocr.reqID, page.Filename); err != nil {
			return errors.New(fmt.Sprintf("Failed to upload image: [%s]", err.Error()))
		}
	}

	return nil
}

func awsUploadImagesConcurrently(ocr ocrInfo) error {
	uploader := s3manager.NewUploader(sess)

	workers, err := strconv.Atoi(config.concurrentUploads.value)

	switch {
	case err != nil:
		workers = 1
	case workers == 0:
		workers = runtime.NumCPU()
	default:
		workers = 1
	}

	logger.Printf("concurrent uploads set to [%s]; limiting to %d uploads", config.concurrentUploads.value, workers)

	wp := workerpool.New(workers)

	start := time.Now()

	uploadFailed := false

	for i, _ := range ocr.ts.Pages {
		page := &ocr.ts.Pages[i]
		wp.Submit(func() {
			if err := awsUploadImage(uploader, ocr.reqID, page.Filename); err != nil {
				uploadFailed = true
				logger.Printf("Failed to upload image: [%s]", err.Error())
			}
		})
	}

	logger.Printf("Waiting for %d uploads to complete...", len(ocr.ts.Pages))

	wp.StopWait()

	if uploadFailed == true {
		logger.Printf("one or more images failed to upload")
		return errors.New("One or more images failed to upload")
	}

	elapsed := time.Since(start).Seconds()

	logger.Printf("%d images uploaded in %0.2f seconds (%0.2f seconds/image)", len(ocr.ts.Pages), elapsed, elapsed/float64(len(ocr.ts.Pages)))

	return nil
}

func awsGenerateOcr(ocr ocrInfo) error {
	if err := awsUploadImagesConcurrently(ocr); err != nil {
		return errors.New(fmt.Sprintf("Upload failed: [%s]", err.Error()))
	}

	req := workflowRequest{}

	req.Pid = ocr.req.pid
	req.Path = ocr.subDir
	req.Lang = ocr.ts.Pid.OcrLanguageHint
	req.ReqID = ocr.reqID
	req.Bucket = config.awsBucketName.value

	for _, page := range ocr.ts.Pages {
		req.Pages = append(req.Pages, ocrPageInfo{Pid: page.Pid, Filename: page.Filename})
	}

	if err := awsSubmitWorkflow(req); err != nil {
		awsDeleteImages(ocr.reqID)
		return errors.New(fmt.Sprintf("Workflow failed: [%s]", err.Error()))
	}

	return nil
}
