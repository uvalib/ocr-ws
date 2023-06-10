package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/swf"
	"github.com/gammazero/workerpool"
)

// holds information we extract from a decision task
type decisionInfo struct {
	input        string
	req          workflowRequest
	taskToken    string
	workflowID   string
	lastEventID  int64
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
	Scale     string `json:"scale,omitempty"`     // converted image scale factor
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
			SetStartToCloseTimeout(config.awsLambdaTimeout.value).
			SetId(randomID()).
			SetInput(input))

	return decision
}

func awsStartTimer(duration int, control string) *swf.Decision {
	decision := (&swf.Decision{}).
		SetDecisionType("StartTimer").
		SetStartTimerDecisionAttributes((&swf.StartTimerDecisionAttributes{}).
			SetControl(control).
			SetStartToFireTimeout(strconv.Itoa(duration)).
			SetTimerId(randomID()))

	return decision
}

func awsEventWithID(events []*swf.HistoryEvent, eventID int64) *swf.HistoryEvent {
	// event n seems to always be at index n-1 in the event history, but
	// in the absence of documentation of this, we check the list to be safe

	for _, e := range events {
		if eventID == *e.EventId {
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

func (c *clientContext) awsFinalizeSuccess(info decisionInfo) {
	res := ocrResultsInfo{}

	res.pid = info.req.Pid
	res.reqid = info.req.ReqID
	res.workDir = getWorkDir(info.req.Path)
	res.overwrite = true

	for _, e := range info.ocrResults {
		// lambda result is json embedded within a json string value; must unmarshal twice
		a := e.LambdaFunctionCompletedEventAttributes

		var input string

		if jErr := json.Unmarshal([]byte(*a.Result), &input); jErr != nil {
			c.err("[AWS] Unmarshal() failed [finalize response intermediate]: %s", jErr.Error())
			continue
		}

		lambdaRes := lambdaResponse{}

		if jErr := json.Unmarshal([]byte(input), &lambdaRes); jErr != nil {
			c.err("[AWS] Unmarshal() failed [finalize response final]: %s", jErr.Error())
			continue
		}

		// get pid from original request
		o := awsEventWithID(info.allEvents, *a.ScheduledEventId)
		origLambdaInput := *o.LambdaFunctionScheduledEventAttributes.Input

		lambdaReq := lambdaRequest{}

		if jErr := json.Unmarshal([]byte(origLambdaInput), &lambdaReq); jErr != nil {
			c.err("[AWS] Unmarshal() failed [finalize request]: %s", jErr.Error())
			continue
		}

		res.pages = append(res.pages, ocrPidInfo{pid: lambdaReq.Pid, text: strings.TrimSpace(lambdaRes.Text)})
	}

	// sort by pid
	sort.Slice(res.pages, func(i, j int) bool { return res.pages[i].pid < res.pages[j].pid })

	go c.processOcrSuccess(res)

	c.awsDeleteImages(info.req.ReqID)
}

func (c *clientContext) awsFinalizeFailure(info decisionInfo, details string) {
	res := ocrResultsInfo{}

	res.pid = info.req.Pid
	res.reqid = info.req.ReqID
	res.details = details
	res.workDir = getWorkDir(info.req.Path)

	go c.processOcrFailure(res)

	c.awsDeleteImages(info.req.ReqID)
}

func (c *clientContext) awsHandleDecisionTask(svc *swf.SWF, info decisionInfo) {
	workflowHalted := false

	for _, e := range info.allEvents {
		t := *e.EventType

		// extract the original input string containing pids that were processed
		if info.input == "" && t == "WorkflowExecutionStarted" {
			info.input = c.decodeWorkflowInput(*e.WorkflowExecutionStartedEventAttributes.Input)
			json.Unmarshal([]byte(info.input), &info.req)
			pages := []ocrPageInfo{}
			for _, p := range info.req.Pages {
				if p.Pid != "" {
					pages = append(pages, p)
				}
			}
			info.req.Pages = pages
			//c.info("[AWS] [%s] input = [%s] (%d pids)", info.workflowID, info.input, len(info.req.Pages))
			c.info("[AWS] [%s] reqid: [%s]  pages: %d", info.workflowID, info.req.ReqID, len(info.req.Pages))
		}

		// collect the completed (successful) OCR events, which contain the OCR results
		if t == "LambdaFunctionCompleted" {
			//c.info("[AWS] [%s] lambda completed", info.workflowID)
			info.ocrResults = append(info.ocrResults, e)
		}

		// set a flag if any workflow execution event failed (start, complete, fail)
		if strings.Contains(t, "WorkflowExecutionFailed") {
			workflowHalted = true
		}

		// from here on out, only consider recent events (events that occurred since
		// since the last time a decision task for this workflow was processed)
		if *e.EventId <= info.lastEventID {
			continue
		}

		// collect all non-decision recent events
		if strings.HasPrefix(t, "Decision") {
			continue
		}

		info.recentEvents = append(info.recentEvents, e)
	}

	c.reqUpdateImagesComplete(getWorkDir(info.req.Path), info.req.ReqID, len(info.ocrResults))

	if workflowHalted {
		c.info("[AWS] [%s] WORKFLOW WAS PREVIOUSLY HALTED", info.workflowID)
	}

	c.info("[AWS] [%s] lambdas completed: %d / %d", info.workflowID, len(info.ocrResults), len(info.req.Pages))

	recentCounts := make(map[string]int)
	var lastEventType string
	for _, e := range info.recentEvents {
		recentCounts[*e.EventType]++
		lastEventType = *e.EventType
	}
	c.info("[AWS] [%s] recent events: %s", info.workflowID, countsToString(recentCounts))
	c.info("[AWS] [%s] last event type: [%s]", info.workflowID, lastEventType)

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
		c.awsFinalizeFailure(info, "No PIDs to process")

	// start of workflow
	// decision(s): schedule a lambda for each pid.  if no pids, fail the workflow
	//case lastEventType == "WorkflowExecutionStarted":
	case recentCounts["WorkflowExecutionStarted"] > 0:
		//c.info("[AWS] [%s] input = [%s] (%d pids)", info.workflowID, info.input, len(info.req.Pages))
		c.info("[AWS] [%s] scheduling %d lambdas...", info.workflowID, len(info.req.Pages))

		for _, page := range info.req.Pages {
			req := lambdaRequest{}

			req.Lang = info.req.Lang
			req.Scale = "100"
			req.Bucket = info.req.Bucket
			req.Key = getS3Filename(info.req.ReqID, page.Filename)
			req.ParentPid = info.req.Pid
			req.Pid = page.Pid

			input, jsonErr := json.Marshal(req)
			if jsonErr != nil {
				c.err("[AWS] [%s] JSON marshal failed: [%s]", info.workflowID, jsonErr.Error())
				decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "lambda creation failed"))
				c.awsFinalizeFailure(info, "OCR generation process failed (initialization failed)")
				break
			}

			//c.info("[AWS] [%s] lambda json: [%s]", info.workflowID, input)

			decisions = append(decisions, awsScheduleLambdaFunction(string(input), "1"))
		}

	// completion condition (success): number of successful lambda executions = number of pids
	// decision: complete the workflow
	case len(info.ocrResults) == len(info.req.Pages):
		// did a previous completion attempt fail?  try, try again
		if e := awsEventWithType(info.recentEvents, "CompleteWorkflowExecutionFailed"); e != nil {
			a := e.CompleteWorkflowExecutionFailedEventAttributes
			c.err("[AWS] [%s] complete workflow execution failed (%s)", info.workflowID, *a.Cause)
			decisions = append([]*swf.Decision{}, awsCompleteWorkflowExecution("SUCCESS"))
		} else {
			decisions = append([]*swf.Decision{}, awsCompleteWorkflowExecution("success"))
			c.awsFinalizeSuccess(info)
		}

	// middle of the workflow -- typically this occurs when lambdas complete/fail/timeout
	// decision(s): if lambdas recently failed/timed out, schedule them to be rerun; otherwise
	// send an empty decision (waits for another event to prompt a new decision task)
	default:

	EventsProcessingLoop:
		for _, e := range info.recentEvents {
			t := *e.EventType

			var origLambdaEvent int64
			var origLambdaInput string
			var origLambdaCount string

			lambdaTimedOut := false

			// attempt to start the workflow failed: ???
			// decision(s): ???
			if t == "WorkflowExecutionFailed" {
				a := e.WorkflowExecutionFailedEventAttributes
				c.err("[AWS] [%s] start workflow execution failed (%s) - (%s)", info.workflowID, *a.Reason, *a.Details)
				decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "workflow execution failed"))
				c.awsFinalizeFailure(info, "OCR generation process failed (could not start process)")
				break EventsProcessingLoop
			}

			/*
				// HANDLED ABOVE in the successful completion condition, as we will never reach this point if this occurs

				// attempt to complete the workflow failed: ???
				// decision(s): ???
				if t == "CompleteWorkflowExecutionFailed" {
					a := e.CompleteWorkflowExecutionFailedEventAttributes
					c.err("[AWS] [%s] complete workflow execution failed (%s)", info.workflowID, *a.Cause)
					decisions = append([]*swf.Decision{}, awsCompleteWorkflowExecution("SUCCESS"))
					break EventsProcessingLoop
				}
			*/

			// attempt to fail the workflow failed: ???
			// decision(s): ???
			if t == "FailWorkflowExecutionFailed" {
				a := e.FailWorkflowExecutionFailedEventAttributes
				c.err("[AWS] [%s] fail workflow execution failed (%s)", info.workflowID, *a.Cause)
				decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("FAILURE", "fail workflow execution failed"))
				break EventsProcessingLoop
			}

			// attempt to start a timer failed: ???
			// decision(s): ???
			if t == "StartTimerFailed" {
				a := e.StartTimerFailedEventAttributes
				c.err("[AWS] [%s] start timer failed (%s)", info.workflowID, *a.Cause)
				continue EventsProcessingLoop
			}

			// signal sent to workflow
			// decisions(s): ???
			if t == "WorkflowExecutionSignaled" {
				a := e.WorkflowExecutionSignaledEventAttributes
				c.info("[AWS] [%s] workflow execution signaled (%s) - (%s)", info.workflowID, *a.SignalName, c.decodeWorkflowInput(*a.Input))
				continue EventsProcessingLoop
			}

			// cancel request sent to workflow
			// decisions(s): ???
			if t == "WorkflowExecutionCancelRequested" {
				//a := e.WorkflowExecutionCancelRequestedEventAttributes
				c.info("[AWS] [%s] workflow cancellation requested", info.workflowID)
				decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "workflow execution canceled"))
				c.awsFinalizeFailure(info, "OCR generation process failed (process was canceled)")
				break EventsProcessingLoop
			}

			// if this a recently failed lambda execution, determine what to do with it
			if t == "LambdaFunctionFailed" {
				a := e.LambdaFunctionFailedEventAttributes
				reason := *a.Reason

				details := lambdaFailureDetails{}
				json.Unmarshal([]byte(*a.Details), &details)

				if details.ErrorType != "" || details.ErrorMessage != "" {
					c.err("[AWS] [%s] lambda failed: (%s) : [%s] / [%s]", info.workflowID, reason, details.ErrorType, details.ErrorMessage)
				} else {
					c.err("[AWS] [%s] lambda failed: (%s)", info.workflowID, reason)
				}

				o := awsEventWithID(info.allEvents, *a.ScheduledEventId)
				origLambdaCount = *o.LambdaFunctionScheduledEventAttributes.Control
				origLambdaEvent = *a.ScheduledEventId
			}

			// if this a recently timed out lambda execution, rerun it
			if t == "LambdaFunctionTimedOut" {
				a := e.LambdaFunctionTimedOutEventAttributes

				o := awsEventWithID(info.allEvents, *a.ScheduledEventId)
				origLambdaCount = *o.LambdaFunctionScheduledEventAttributes.Control
				origLambdaEvent = *a.ScheduledEventId
				lambdaTimedOut = true

				timeoutStr := ""
				if o.LambdaFunctionScheduledEventAttributes.StartToCloseTimeout != nil {
					timeoutStr = fmt.Sprintf(" after %s seconds", *o.LambdaFunctionScheduledEventAttributes.StartToCloseTimeout)
				}

				c.err("[AWS] [%s] lambda timed out%s (%s)", info.workflowID, timeoutStr, *a.TimeoutType)
			}

			// if this a timer that fired, rerun the associated lambda
			if t == "TimerFired" {
				a := e.TimerFiredEventAttributes

				c.info("[AWS] [%s] timer fired", info.workflowID)

				o := awsEventWithID(info.allEvents, *a.StartedEventId)

				id, _ := strconv.Atoi(*o.TimerStartedEventAttributes.Control)
				o = awsEventWithID(info.allEvents, int64(id))

				origLambdaCount = *o.LambdaFunctionScheduledEventAttributes.Control
				origLambdaInput = *o.LambdaFunctionScheduledEventAttributes.Input
				origLambdaEvent = int64(id)
			}

			// handle lambda retry-related scenarios:
			// if just count/event is set, start a new timer to delay lambda retry.  if input is also set, rerun the lambda.
			if origLambdaCount != "" && origLambdaEvent != 0 {
				count, _ := strconv.Atoi(origLambdaCount)

				if origLambdaInput != "" {
					// rerun the referenced lambda, with reduced scale only if the lambda timed out

					count++

					newLambdaInput := origLambdaInput

					if lambdaTimedOut == true {
						req := lambdaRequest{}

						if jErr := json.Unmarshal([]byte(origLambdaInput), &req); jErr != nil {
							c.err("[AWS] Unmarshal() failed [lambda retry]: %s", jErr.Error())
							decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "lambda unmarshal failed"))
							c.awsFinalizeFailure(info, "OCR generation process failed (retry failed)")
							break
						}

						// reduce scale in steps of 10%, going no lower than 10%
						scale, _ := strconv.Atoi(req.Scale)
						newScale := fmt.Sprintf("%d", maxOf(10, scale-10))
						c.info("[AWS] [%s] scale: %s%% -> %s%%", info.workflowID, req.Scale, newScale)
						req.Scale = newScale

						input, jErr := json.Marshal(req)
						if jErr != nil {
							c.err("[AWS] [%s] JSON marshal failed: [%s]", info.workflowID, jErr.Error())
							decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "lambda re-marshal failed"))
							c.awsFinalizeFailure(info, "OCR generation process failed (retry failed)")
							break
						}

						newLambdaInput = string(input)

						c.info("[AWS] [%s] new input: %s", info.workflowID, newLambdaInput)
					}

					c.info("[AWS] [%s] retrying lambda event %d (attempt %d)", info.workflowID, origLambdaEvent, count)

					decisions = append(decisions, awsScheduleLambdaFunction(newLambdaInput, strconv.Itoa(count)))
				} else {
					// start a timer referencing the original lambda to be rerun, with exponential backoff based on execution count

					maxAttempts, _ := strconv.Atoi(config.lambdaAttempts.value)
					if maxAttempts < 1 {
						maxAttempts = 1
					}

					// limit number of reruns
					if count >= maxAttempts {
						c.err("[AWS] [%s] maximum lambda attempts reached (%d); failing", info.workflowID, maxAttempts)
						decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure", "maximum OCR attempts reached for one or more pages"))
						c.awsFinalizeFailure(info, "OCR generation process failed (maximum attempts reached)")
						break EventsProcessingLoop
					}

					delay := int(math.Pow(2, float64(count))) + randpool.Intn(30)

					c.info("[AWS] [%s] scheduling lambda event %d to be retried in %d seconds...", info.workflowID, origLambdaEvent, delay)

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
			c.err("[AWS] [%s] decision validation error: [%s]", info.workflowID, err.Error())
			return
		}
	}
	c.info("[AWS] [%s] decision(s): %s", info.workflowID, countsToString(decisionCounts))

	// build, validate, and send response

	respParams := (&swf.RespondDecisionTaskCompletedInput{}).
		SetDecisions(decisions).
		SetTaskToken(info.taskToken)

	if err := respParams.Validate(); err != nil {
		c.err("[AWS] [%s] respond validation error: [%s]", info.workflowID, err.Error())
		return
	}

	_, respErr := svc.RespondDecisionTaskCompleted(respParams)

	if respErr != nil {
		c.err("[AWS] [%s] respond error: [%s]", info.workflowID, respErr.Error())
		return
	}
}

func (c *clientContext) awsPollForDecisionTasks() {
	svc := swf.New(sess)

	for {
		var info decisionInfo

		c.info("[AWS] polling for decision task...")

		pollParams := (&swf.PollForDecisionTaskInput{}).
			SetDomain(config.awsSwfDomain.value).
			SetTaskList((&swf.TaskList{}).
				SetName(config.awsSwfTaskList.value))

		// iterate over pages, collecting initial workflow information to process later
		pollErr := svc.PollForDecisionTaskPages(pollParams,
			func(page *swf.PollForDecisionTaskOutput, lastPage bool) bool {
				if page.PreviousStartedEventId != nil {
					info.lastEventID = *page.PreviousStartedEventId
				}

				if info.taskToken == "" && page.TaskToken != nil {
					info.taskToken = *page.TaskToken
					//c.info("[AWS] TaskToken  = [%s]", info.taskToken)
				}

				if info.workflowID == "" && page.WorkflowExecution != nil {
					info.workflowID = *page.WorkflowExecution.WorkflowId
					c.info("[AWS] [%s] <-- working decision from this workflow", info.workflowID)
				}

				info.allEvents = append(info.allEvents, page.Events...)

				return true
			})

		if pollErr != nil {
			c.err("[AWS] polling error: %s", pollErr.Error())
			time.Sleep(60 * time.Second)
			continue
		}

		if info.taskToken == "" {
			c.info("[AWS] no decision tasks available")
			continue
		}

		// process this decision task
		c.awsHandleDecisionTask(svc, info)
	}
}

func (c *clientContext) awsWorkflowInList(ExecutionInfos []*swf.WorkflowExecutionInfo, workflowID, runID string) bool {
	for _, e := range ExecutionInfos {
		c.info("[AWS] checking WorkflowId: [%s] / RunId: [%s]", *e.Execution.WorkflowId, *e.Execution.RunId)

		if *e.Execution.WorkflowId == workflowID && *e.Execution.RunId == runID {
			return true
		}
	}

	return false
}

func awsListWorkflowDateRange() (time.Time, time.Time) {
	// set window based on configured workflow timeout (in seconds), plus a little padding
	secs, _ := strconv.Atoi(config.awsSwfWorkflowTimeout.value)
	secs += 300

	now := time.Now()
	then := now.Add(time.Duration(-secs) * time.Second)

	return then, now
}

func (c *clientContext) awsWorkflowIsOpen(workflowID, runID string) (bool, error) {
	c.info("[AWS] checking for open workflow: [%s]", workflowID)

	svc := swf.New(sess)

	from, to := awsListWorkflowDateRange()

	input := (&swf.ListOpenWorkflowExecutionsInput{}).
		SetDomain(config.awsSwfDomain.value).
		SetExecutionFilter((&swf.WorkflowExecutionFilter{}).
			SetWorkflowId(workflowID)).
		SetStartTimeFilter((&swf.ExecutionTimeFilter{}).
			SetOldestDate(from).
			SetLatestDate(to))

	res, err := svc.ListOpenWorkflowExecutions(input)

	if err != nil {
		c.err("[AWS] list open workflows error: [%s]", err.Error())
		return false, errors.New("failed to list open workflows")
	}

	return c.awsWorkflowInList(res.ExecutionInfos, workflowID, runID), nil
}

func (c *clientContext) awsWorkflowIsClosed(workflowID, runID string) (bool, error) {
	c.info("[AWS] checking for closed workflow: [%s]", workflowID)

	svc := swf.New(sess)

	from, to := awsListWorkflowDateRange()

	input := (&swf.ListClosedWorkflowExecutionsInput{}).
		SetDomain(config.awsSwfDomain.value).
		SetExecutionFilter((&swf.WorkflowExecutionFilter{}).
			SetWorkflowId(workflowID)).
		SetStartTimeFilter((&swf.ExecutionTimeFilter{}).
			SetOldestDate(from).
			SetLatestDate(to))

	res, err := svc.ListClosedWorkflowExecutions(input)

	if err != nil {
		c.err("[AWS] list closed workflows error: [%s]", err.Error())
		return false, errors.New("failed to list closed workflows")
	}

	return c.awsWorkflowInList(res.ExecutionInfos, workflowID, runID), nil
}

func (c *clientContext) awsDeleteImages(reqDir string) error {
	c.info("[AWS] relying on S3 policies to remove original images")
	return nil

	/*
		svc := s3.New(sess)

		c.info("[AWS] deleting: [%s]", reqDir)

		iter := s3manager.NewDeleteListIterator(svc, &s3.ListObjectsInput{
			Bucket: aws.String(config.awsBucketName.value),
			Prefix: aws.String(fmt.Sprintf("requests/%s", reqDir)),
		})

		err := s3manager.NewBatchDeleteWithClient(svc).Delete(aws.BackgroundContext(), iter)

		if err != nil {
			c.err("[AWS] S3 delete failed: [%s]", err.Error())
		}

		return err
	*/
}

func (c *clientContext) awsSubmitWorkflow(req workflowRequest) error {
	svc := swf.New(sess)

	id := randomID()

	input, jsonErr := json.Marshal(req)
	if jsonErr != nil {
		c.err("[AWS] workflow request JSON marshal failed: [%s]", jsonErr.Error())
		return errors.New("failed to encode workflow request")
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
		SetInput(c.encodeWorkflowInput(string(input)))

	res, startErr := svc.StartWorkflowExecution(startParams)

	if startErr != nil {
		c.err("[AWS] start workflow error: [%s]", startErr.Error())
		return errors.New("failed to start OCR workflow")
	}

	c.info("[AWS] started WorkflowId [%s] with RunId: [%s]", id, *res.RunId)

	c.reqUpdateAwsWorkflowID(getWorkDir(req.Path), req.ReqID, id)
	c.reqUpdateAwsRunID(getWorkDir(req.Path), req.ReqID, *res.RunId)

	return nil
}

func (c *clientContext) openURL(url string) (io.ReadCloser, error) {
	maxTries := 5
	backoff := 1

	for i := 1; i <= maxTries; i++ {
		h, err := http.Get(url)

		if err != nil {
			return nil, err
		}

		if h.StatusCode == http.StatusOK {
			return h.Body, nil
		}

		if h.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("received http status: %s", h.Status)
		}

		if i == maxTries {
			c.err("[AWS] open [%s] (try %d/%d): received status: %s; giving up", url, i, maxTries, h.Status)
			return nil, fmt.Errorf("max tries reached")
		}

		c.info("[AWS] open [%s] (try %d/%d): received status: %s; will try again in %d seconds...", url, i, maxTries, h.Status, backoff)

		time.Sleep(time.Duration(backoff) * time.Second)
		backoff *= 2
	}

	return nil, fmt.Errorf("max tries reached")
}

func (c *clientContext) awsUploadImage(uploader *s3manager.Uploader, reqID, imageSource, remoteName string) error {
	s3File := getS3Filename(reqID, remoteName)

	var imageStream io.ReadCloser
	var err error

	if strings.HasPrefix(imageSource, "/") {
		imageStream, err = os.Open(imageSource)
	} else {
		imageStream, err = c.openURL(imageSource)
	}

	if err != nil {
		return err
	}

	if imageStream == nil {
		return errors.New("failed to upload image")
	}

	defer imageStream.Close()

	c.info("[AWS] uploading: [%s] => [%s]", imageSource, s3File)

	_, aerr := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(config.awsBucketName.value),
		Key:    aws.String(s3File),
		Body:   imageStream,
	})

	return aerr
}

func (c *clientContext) awsUploadImagesConcurrently() error {
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

	c.info("[AWS] concurrent uploads set to [%s]; limiting to %d uploads", config.concurrentUploads.value, workers)

	wp := workerpool.New(workers)

	start := time.Now()

	uploadCount := 0
	uploadFailed := false

	mutex := &sync.Mutex{}

	for i := range c.ocr.ts.Pages {
		page := &c.ocr.ts.Pages[i]
		wp.Submit(func() {
			if err := c.awsUploadImage(uploader, c.ocr.reqID, page.imageSource, page.remoteName); err != nil {
				uploadFailed = true
				c.err("[AWS] Failed to upload image: [%s]", err.Error())
			} else {
				mutex.Lock()
				uploadCount++
				c.reqUpdateImagesUploaded(c.ocr.workDir, c.ocr.reqID, uploadCount)
				mutex.Unlock()
			}
		})
	}

	c.info("[AWS] Waiting for %d uploads to complete...", len(c.ocr.ts.Pages))

	wp.StopWait()

	if uploadFailed == true {
		c.err("[AWS] one or more images failed to upload")
		return errors.New("one or more images failed to upload")
	}

	elapsed := time.Since(start).Seconds()

	c.info("[AWS] %d images uploaded in %0.2f seconds (%0.2f seconds/image)", len(c.ocr.ts.Pages), elapsed, elapsed/float64(len(c.ocr.ts.Pages)))

	return nil
}

func (c *clientContext) awsGenerateOcr() error {
	if config.awsDisabled.value == true {
		return fmt.Errorf("automatically failed: [AWS is disabled]")
	}

	// create {local tif or iiif url} to {s3 key} mapping
	for i := range c.ocr.ts.Pages {
		page := &c.ocr.ts.Pages[i]

		localFile := getLocalFilename(page.Filename)

		if _, err := os.Stat(localFile); err == nil {
			page.imageSource = localFile
		} else {
			page.imageSource = getIIIFUrl(page.Pid)
		}

		page.remoteName = getRemoteFilename(page.Filename, page.imageSource)

		c.info("[AWS] mapping [%s] => [%s]", page.imageSource, page.remoteName)
	}

	if err := c.awsUploadImagesConcurrently(); err != nil {
		return fmt.Errorf("upload failed: [%s]", err.Error())
	}

	req := workflowRequest{}

	req.Pid = c.req.pid
	req.Path = c.ocr.subDir
	req.Lang = c.ocr.ts.Pid.OcrLanguageHint
	req.ReqID = c.ocr.reqID
	req.Bucket = config.awsBucketName.value

	for _, page := range c.ocr.ts.Pages {
		req.Pages = append(req.Pages, ocrPageInfo{Pid: page.Pid, Filename: page.remoteName})
	}

	if err := c.awsSubmitWorkflow(req); err != nil {
		c.awsDeleteImages(c.ocr.reqID)
		return fmt.Errorf("workflow failed: [%s]", err.Error())
	}

	return nil
}

func (c *clientContext) encodeWorkflowInput(input string) string {
	// attempt to encode input as a base64-encoded gzipped string.
	// if that fails, just return the original input

	//c.info("encoding (%5d) : [%s]", len(input), input)

	var buf bytes.Buffer

	gzEnc := gzip.NewWriter(&buf)
	if _, gzErr := gzEnc.Write([]byte(input)); gzErr != nil {
		c.warn("encode: gzip write error: %s", gzErr.Error())
		return input
	}

	if gzErr := gzEnc.Close(); gzErr != nil {
		c.warn("encode: gzip close error: %s", gzErr.Error())
		return input
	}

	enc := base64.StdEncoding.EncodeToString([]byte(buf.String()))

	//c.info("encoded (%5d) : [%s]", len(enc), enc)

	c.info("encode: compressed input from %d bytes to %d bytes", len(input), len(enc))

	return enc
}

func (c *clientContext) decodeWorkflowInput(input string) string {
	// attempt to decode input as a base64-encoded gzipped string.
	// if that fails, just return the original input

	//c.info("decoding (%5d) : [%s]", len(input), input)

	b64, b64Err := base64.StdEncoding.DecodeString(input)
	if b64Err != nil {
		c.warn("decode: base64 read error: %s", b64Err.Error())
		return input
	}

	gzDec, gzErr := gzip.NewReader(bytes.NewReader([]byte(b64)))
	if gzErr != nil {
		c.warn("decode: gzip init error: %s", gzErr.Error())
		return input
	}

	dec, decErr := ioutil.ReadAll(gzDec)
	if decErr != nil {
		c.warn("decode: gzip read error: %s", decErr.Error())
		return input
	}

	//c.info("decoded (%5d) : [%s]", len(dec), dec)

	c.info("decode: decompressed input from %d bytes to %d bytes", len(input), len(dec))

	return string(dec)
}
