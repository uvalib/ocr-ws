package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/swf"
	"github.com/satori/go.uuid"
)

// holds information we extract from a decision task
type decisionInfo struct {
	input string
	req workflowRequest
	taskToken string
	workflowId string
	lastEventId int64
	allEvents []*swf.HistoryEvent
	recentEvents []*swf.HistoryEvent
	ocrResults []*swf.HistoryEvent
}

// json for inter-workflow communication
type ocrPageInfo struct {
	Lang string `json:"lang,omitempty"`
	Pid string `json:"pid,omitempty"`
}

type workflowRequest struct {
	Pages []ocrPageInfo `json:"pages,omitempty"`
}

type lambdaRequest struct {
	Lang string `json:"lang,omitempty"`
	Url string `json:"url,omitempty"`
}

type lambdaResponse struct {
	Text string `json:"text,omitempty`
}

// json for failed lambda error details
type lambdaFailureDetails struct {
	ErrorMessage string `json:"errorMessage,omitempty"`
	ErrorType string `json:"errorType,omitempty"`
	StackTrace []string `json:"stackTrace,omitempty"`
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

	for i, e := range events {
		if eventId == *e.EventId {
			logger.Printf("found event id %d at index %d", eventId, i)
			return e
		}
	}

	return nil
}

func awsFinalizeResults(info decisionInfo) {
	for i, e:= range info.ocrResults {
		logger.Printf("ocrResult[%d]: %s\n%s\n", i, *e.EventType, *e.LambdaFunctionCompletedEventAttributes.Result)
	}
}

func awsHandleDecisionTask(svc *swf.SWF, info decisionInfo) {
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
			logger.Printf("input      = [%s] (%d pids)",info.input,len(info.req.Pages))
		}

		// collect the completed (successful) OCR events, which contain the OCR results
		if t == "LambdaFunctionCompleted" {
			logger.Printf("lambda completed")
			info.ocrResults = append(info.ocrResults, e)
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

	logger.Printf("lambdas completed: %d / %d", len(info.ocrResults), len(info.req.Pages))

	lastEventType := *info.recentEvents[len(info.recentEvents)-1].EventType
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
		decisions = append(decisions, awsFailWorkflowExecution("failure","No PIDs to process"))

	// start of workflow
	// decision(s): schedule a lambda for each pid.  if no pids, fail the workflow
	case lastEventType == "WorkflowExecutionStarted":
		url := config.iiifUrlTemplate.value

		for _, page := range info.req.Pages {
			req := lambdaRequest{}
			req.Url = strings.Replace(url, "{PID}", page.Pid, 1)
			input := fmt.Sprintf(`{ "args": "-l eng", "url": "%s" }`, req.Url)
			decisions = append(decisions, awsScheduleLambdaFunction(input))
		}

	// completion condition (success): number of successful lambda executions = number of pids
	// decision: complete the workflow
	case len(info.ocrResults) == len(info.req.Pages):
		decisions = append(decisions, awsCompleteWorkflowExecution("success"))
		go awsFinalizeResults(info)

	// middle of the workflow -- typically this occurs when lambdas complete/fail/timeout
	// decision(s): if lambdas recently failed/timed out, schedule them to be rerun; otherwise
	// send an empty decision (waits for another event to prompt a new decision task)
	// NOTE: there is no workflow failure condition for lambdas that fail/timeout, we simply
	// keep retrying them until the workflow itself times out.
	default:
		for _, e := range info.recentEvents {
			t := *e.EventType

			var rerunInput string

			// if this a recently failed lambda execution, determine what to do with it
			if t == "LambdaFunctionFailed" {
				logger.Printf("lambda failed (%s)", *e.LambdaFunctionFailedEventAttributes.Reason)
				details := lambdaFailureDetails{}
				json.Unmarshal([]byte(*e.LambdaFunctionFailedEventAttributes.Details), &details)

				logger.Printf("lambda failed: [%s] / [%s]", details.ErrorType, details.ErrorMessage)

				decisions = append([]*swf.Decision{}, awsFailWorkflowExecution("failure","one or more pages failed"))
				break
//				origEvent := awsEventWithId(info.allEvents, *e.LambdaFunctionFailedEventAttributes.ScheduledEventId)
//				rerunInput = *origEvent.LambdaFunctionScheduledEventAttributes.Input
			}

			// if this a recently timed out lambda execution, rerun it
			if t == "LambdaFunctionTimedOut" {
				logger.Printf("lambda timed out (%s)", *e.LambdaFunctionTimedOutEventAttributes.TimeoutType)
				origEvent := awsEventWithId(info.allEvents, *e.LambdaFunctionTimedOutEventAttributes.ScheduledEventId)
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

//	logger.Printf("respond response: [%s]", resp.GoString())
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
					logger.Printf("TaskToken  = [%s]", info.taskToken)
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
		return errors.New("Failed to encoding workflow request")
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

func awsSubmitTestWorkflows() {
	req := workflowRequest{
		Pages: []ocrPageInfo{
			ocrPageInfo{Lang: "eng", Pid: "uva-lib:2555271"},
			ocrPageInfo{Lang: "huh", Pid: ""},
			ocrPageInfo{Lang: "eng", Pid: "uva-lib:2555272"},
			ocrPageInfo{Lang: "wat"},
			ocrPageInfo{Lang: "eng", Pid: "tsm:1808296"},
			ocrPageInfo{Lang: "eng", Pid: "uva-lib:2555273"},
		},
	}

	awsSubmitWorkflow(req)

	return

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	numWorkFlows := 1

	for i := 0; i < numWorkFlows; i++ {
		s := r1.Intn(45) + 15
		time.Sleep(time.Duration(s) * time.Second)

		req := workflowRequest{
			Pages: []ocrPageInfo{
				ocrPageInfo{Lang: "eng", Pid: "uva-lib:2555271"},
				ocrPageInfo{Lang: "eng", Pid: "uva-lib:2555272"},
				ocrPageInfo{Lang: "eng", Pid: "tsm:1808296"},
				ocrPageInfo{Lang: "eng", Pid: "uva-lib:2555273"},
			},
		}

		awsSubmitWorkflow(req)
	}
}
