package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/swf"
	"github.com/satori/go.uuid"
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

// json for inter-workflow communication
type ocrPageInfo struct {
	Pid      string `json:"pid,omitempty"`
	Filename string `json:"filename,omitempty"`
}

type workflowRequest struct {
	Pid   string        `json:"pid,omitempty"`
	Path  string        `json:"path,omitempty"`
	Lang  string        `json:"lang,omitempty"`
	Pages []ocrPageInfo `json:"pages,omitempty"`
}

type lambdaRequest struct {
	Pid  string `json:"pid,omitempty"`
	Lang string `json:"lang,omitempty"`
	Args string `json:"args,omitempty"`
	File string `json:"file,omitempty"`
	//Url  string `json:"url,omitempty"`
}

type lambdaResponse struct {
	Pid  string `json:"pid,omitempty`
	Text string `json:"text,omitempty`
}

// json for failed lambda error details
type lambdaFailureDetails struct {
	ErrorMessage string   `json:"errorMessage,omitempty"`
	ErrorType    string   `json:"errorType,omitempty"`
	StackTrace   []string `json:"stackTrace,omitempty"`
}

// functions

func newUUID() string {
	taskUUID := uuid.Must(uuid.NewV4())

	return taskUUID.String()
}

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

func awsScheduleLambdaFunction(input string) *swf.Decision {
	decision := (&swf.Decision{}).
		SetDecisionType("ScheduleLambdaFunction").
		SetScheduleLambdaFunctionDecisionAttributes((&swf.ScheduleLambdaFunctionDecisionAttributes{}).
			SetName(config.awsLambdaFunction.value).
			SetId(newUUID()).
			SetInput(input))

	return decision
}

func awsStartTimer(duration int) *swf.Decision {
	decision := (&swf.Decision{}).
		SetDecisionType("StartTimer").
		SetStartTimerDecisionAttributes((&swf.StartTimerDecisionAttributes{}).
			SetStartToFireTimeout(strconv.Itoa(duration)).
			SetTimerId(newUUID()))

	return decision
}

func awsEventWithId(events []*swf.HistoryEvent, eventId int64) *swf.HistoryEvent {
	// event n seems to always be at index n-1 in the event history, but
	// in the absence of documentation of this, we check the list to be safe

	for _, e := range events {
		if eventId == *e.EventId {
			//logger.Printf("found event id %d at index %d", eventId, i)
			return e
		}
	}

	return nil
}

func awsFinalizeSuccess(info decisionInfo) {
	res := ocrResultsInfo{}

	res.pid = info.req.Pid
	res.workDir = getWorkDir(info.req.Path)

	for i, e := range info.ocrResults {
		// lambda result is json embedded within a json string value; must unmarshal twice
		var tmp string

		if jErr := json.Unmarshal([]byte(*e.LambdaFunctionCompletedEventAttributes.Result), &tmp); jErr != nil {
			logger.Printf("Unmarshal() failed [finalize intermediate]: %s", jErr.Error())
			continue
		}

		lr := lambdaResponse{}

		if jErr := json.Unmarshal([]byte(tmp), &lr); jErr != nil {
			logger.Printf("Unmarshal() failed [finalize final]: %s", jErr.Error())
			continue
		}

		logger.Printf("ocrResult[%d]: PID: [%s]  Text:\n\n%s\n\n", i, lr.Pid, lr.Text)

		res.pages = append(res.pages, ocrPidInfo{pid: lr.Pid, text: lr.Text})
	}

	go processOcrSuccess(res)
}

func awsFinalizeFailure(info decisionInfo, details string) {
	res := ocrResultsInfo{}

	res.pid = info.req.Pid
	res.details = details
	res.workDir = getWorkDir(info.req.Path)

	go processOcrFailure(res)
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
			//logger.Printf("input = [%s] (%d pids)", info.input, len(info.req.Pages))
		}

		// collect the completed (successful) OCR events, which contain the OCR results
		if t == "LambdaFunctionCompleted" {
			//logger.Printf("lambda completed")
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
		logger.Printf("WORKFLOW WAS PREVIOUSLY HALTED")
	}

	logger.Printf("lambdas completed: %d / %d", len(info.ocrResults), len(info.req.Pages))

	lastEvent := info.recentEvents[len(info.recentEvents)-1]
	lastEventType := *lastEvent.EventType
	logger.Printf("last event type: [%s]", lastEventType)

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
	case lastEventType == "WorkflowExecutionStarted":
		logger.Printf("input = [%s] (%d pids)", info.input, len(info.req.Pages))

		for _, page := range info.req.Pages {
			req := lambdaRequest{}
			req.Pid = page.Pid
			req.Lang = info.req.Lang
			req.Args = fmt.Sprintf(`-l %s -p %s`, req.Lang, req.Pid)
			req.File = getS3Filename(page.Filename)

			input, jsonErr := json.Marshal(req)
			if jsonErr != nil {
				logger.Printf("JSON marshal failed: [%s]", jsonErr.Error())
				decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "lambda creation failed"))
				awsFinalizeFailure(info, "OCR generation process initialization failed")
				break
			}

			logger.Printf("lambda json: [%s]", input)

			decisions = append(decisions, awsScheduleLambdaFunction(string(input)))
		}

	// completion condition (success): number of successful lambda executions = number of pids
	// decision: complete the workflow
	case len(info.ocrResults) == len(info.req.Pages):
		decisions = append([]*swf.Decision{}, awsCompleteWorkflowExecution("success"))
		awsFinalizeSuccess(info)

	// middle of the workflow -- typically this occurs when lambdas complete/fail/timeout
	// decision(s): if lambdas recently failed/timed out, schedule them to be rerun; otherwise
	// send an empty decision (waits for another event to prompt a new decision task)
	// NOTE: there is no workflow failure condition for lambdas that fail/timeout, we simply
	// keep retrying them until the workflow itself times out.
	default:
		var uniqueEventTypes []string
		for _, e := range info.recentEvents {
			uniqueEventTypes = appendStringIfMissing(uniqueEventTypes, *e.EventType)
		}
		logger.Printf("unique recent events: [%s]", strings.Join(uniqueEventTypes, ", "))

	EventsProcessingLoop:
		for _, e := range info.recentEvents {
			t := *e.EventType

			var rerunInput string

			// attempt to start the workflow failed: ???
			// decision(s): ???
			if t == "WorkflowExecutionFailed" {
				a := e.WorkflowExecutionFailedEventAttributes
				logger.Printf("start workflow execution failed (%s) - (%s)", *a.Reason, *a.Details)
				decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "workflow execution failed"))
				awsFinalizeFailure(info, "OCR generation process failed")
				break EventsProcessingLoop
			}

			// attempt to complete the workflow failed: ???
			// decision(s): ???
			if t == "CompleteWorkflowExecutionFailed" {
				a := e.CompleteWorkflowExecutionFailedEventAttributes
				logger.Printf("complete workflow execution failed (%s)", *a.Cause)
				decisions = append([]*swf.Decision{}, awsCompleteWorkflowExecution("SUCCESS"))
				break EventsProcessingLoop
			}

			// attempt to fail the workflow failed: ???
			// decision(s): ???
			if t == "FailWorkflowExecutionFailed" {
				a := e.FailWorkflowExecutionFailedEventAttributes
				logger.Printf("fail workflow execution failed (%s)", *a.Cause)
				decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("FAILURE", "fail workflow execution failed"))
				break EventsProcessingLoop
			}

			// if this a recently failed lambda execution, determine what to do with it
			if t == "LambdaFunctionFailed" {
				a := e.LambdaFunctionFailedEventAttributes
				reason := *a.Reason

				details := lambdaFailureDetails{}
				json.Unmarshal([]byte(*a.Details), &details)

				logger.Printf("lambda failed: (%s) : [%s] / [%s]", reason, details.ErrorType, details.ErrorMessage)

				// rerun anything except unhandled errors
				switch {
				case reason == "UnhandledError":
					logger.Printf("unhandled error")
					decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "unhandled lambda error"))
					awsFinalizeFailure(info, "OCR generation process failed unexpectedly")
					break EventsProcessingLoop

				case reason == "HandledError":
					// could be 503 error on IIIF server due to load we put on it
					logger.Printf("handled error")

				case reason == "TooManyRequestsException":
					logger.Printf("too many requests")

				default:
					logger.Printf("some other error")
				}

				origEvent := awsEventWithId(info.allEvents, *a.ScheduledEventId)
				rerunInput = *origEvent.LambdaFunctionScheduledEventAttributes.Input
			}

			// if this a recently timed out lambda execution, rerun it
			if t == "LambdaFunctionTimedOut" {
				a := e.LambdaFunctionTimedOutEventAttributes
				logger.Printf("lambda timed out (%s)", *a.TimeoutType)
				origEvent := awsEventWithId(info.allEvents, *a.ScheduledEventId)
				rerunInput = *origEvent.LambdaFunctionScheduledEventAttributes.Input
			}

			if rerunInput != "" {
				logger.Printf("rerunning lambda with original input: [%s]", rerunInput)
				decisions = append(decisions, awsScheduleLambdaFunction(rerunInput))
			}
		}
	}

	// quick check to ensure all decisions made appear valid
	for _, d := range decisions {
		if err := d.Validate(); err != nil {
			logger.Printf("decision validation error: [%s]", err.Error())
			return
		}

		logger.Printf("decision: [%s]", *d.DecisionType)
	}

	respParams := (&swf.RespondDecisionTaskCompletedInput{}).
		SetDecisions(decisions).
		SetTaskToken(info.taskToken)

	if err := respParams.Validate(); err != nil {
		logger.Printf("respond validation error: [%s]", err.Error())
		return
	}

	_, respErr := svc.RespondDecisionTaskCompleted(respParams)

	if respErr != nil {
		logger.Printf("responding error: [%s]", respErr.Error())
		return
	}

	//logger.Printf("respond response: [%s]", resp.GoString())
}

func awsPollForDecisionTasks() {
	svc := swf.New(sess)

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
					logger.Printf("WorkflowId = [%s]", info.workflowId)
				}

				info.allEvents = append(info.allEvents, page.Events...)

				return true
			})

		if pollErr != nil {
			logger.Printf("polling error: %s", pollErr.Error())
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
		SetLambdaRole(config.awsLambdaRole.value).
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

func awsUploadImage(uploader *s3manager.Uploader, imgFile string) error {
	f, err := os.Open(imgFile)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to open image file: [%s]", err.Error()))
	}
	defer f.Close()

	baseFile := path.Base(imgFile)
	parentDir := path.Base(path.Dir(imgFile))
	s3File := path.Join(parentDir, baseFile)

	logger.Printf("uploading: [%s] => [%s]", imgFile, s3File)

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
		// "000012345_0123.tif" => ("000012345", "0123.tif")
		bits := strings.Split(page.Filename, "_")
		imgFile := fmt.Sprintf("%s/%s/%s", config.archiveDir.value, bits[0], page.Filename)
		if err := awsUploadImage(uploader, imgFile); err != nil {
			return errors.New(fmt.Sprintf("Failed to upload image: [%s]", err.Error()))
		}
	}

	return nil
}

func awsGenerateOcr(ocr ocrInfo) error {
	if err := awsUploadImages(ocr); err != nil {
		return errors.New(fmt.Sprintf("Upload failed: [%s]", err.Error()))
	}

	req := workflowRequest{}

	req.Pid = ocr.req.pid
	req.Path = ocr.subDir
	req.Lang = ocr.ts.OcrLanguageHint

	for _, page := range ocr.ts.Pages {
		req.Pages = append(req.Pages, ocrPageInfo{Pid: page.Pid, Filename: page.Filename})
	}

	if err := awsSubmitWorkflow(req); err != nil {
		return errors.New(fmt.Sprintf("Workflow failed: [%s]", err.Error()))
	}

	return nil
}
