package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/service/swf"
)

func awsPollForDecisions() {
	svc := swf.New(sess)

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	timerid := 1

	for {
		logger.Printf("polling for decision...")

		pageNum := 0

		pollParams := (&swf.PollForDecisionTaskInput{}).
			SetDomain(config.awsSwfDomain.value).
			SetTaskList((&swf.TaskList{}).
				SetName(config.awsSwfTaskList.value))

		var taskToken string

		pollErr := svc.PollForDecisionTaskPages(pollParams,
			func(page *swf.PollForDecisionTaskOutput, lastPage bool) bool {
				pageNum++
				logger.Printf("[page %d]\n%s\n[page %d]", pageNum, page, pageNum)
				if page.TaskToken != nil {
					logger.Printf("TaskToken = [%s]", *page.TaskToken)
					taskToken = *page.TaskToken
				}
				return true
			})

		if pollErr != nil {
			logger.Printf("polling error: %s", pollErr.Error())
			time.Sleep(5 * time.Second)
			continue
		}

		if taskToken == "" {
			logger.Printf("empty task token")
			time.Sleep(5 * time.Second)
			continue
		}

		var decisions []*swf.Decision
		var decision *swf.Decision

		r := r1.Intn(10)

		switch r {
		case 0:
			decision = (&swf.Decision{}).
				SetDecisionType("CompleteWorkflowExecution").
				SetCompleteWorkflowExecutionDecisionAttributes((&swf.CompleteWorkflowExecutionDecisionAttributes{}).
					SetResult("yay"))
		case 1:
			decision = (&swf.Decision{}).
				SetDecisionType("FailWorkflowExecution").
				SetFailWorkflowExecutionDecisionAttributes((&swf.FailWorkflowExecutionDecisionAttributes{}).
					SetReason("boo").
					SetDetails("it broke"))
		case 3:
			decision = (&swf.Decision{}).
				SetDecisionType("ScheduleLambdaFunction").
				SetScheduleLambdaFunctionDecisionAttributes((&swf.ScheduleLambdaFunctionDecisionAttributes{}).
					SetName(config.awsLambdaFunction.value).
					SetId("fixme").
					SetInput(`{ "args": "-d -p -l eng", "url": "https://iiif.lib.virginia.edu/iiif/uva-lib:2555272/full/full/0/default.jpg" }`))
		default:
			decision = (&swf.Decision{}).
				SetDecisionType("StartTimer").
				SetStartTimerDecisionAttributes((&swf.StartTimerDecisionAttributes{}).
					SetStartToFireTimeout("30").
					SetTimerId(fmt.Sprintf("timer%03d",timerid)))
			timerid++
		}

		if err := decision.Validate(); err != nil {
			logger.Printf("decision validation error: [%s]", err.Error())
			time.Sleep(5 * time.Second)
			continue
		}

		logger.Printf("decision: %d => [%s]", r, decision.GoString())
		decisions = append(decisions,decision)

		respParams := (&swf.RespondDecisionTaskCompletedInput{}).
			SetDecisions(decisions).
			SetTaskToken(taskToken)

		if err := respParams.Validate(); err != nil {
			logger.Printf("respond validation error: [%s]", err.Error())
			time.Sleep(5 * time.Second)
			continue
		}

		resp, respErr := svc.RespondDecisionTaskCompleted(respParams)

		if respErr != nil {
			logger.Printf("responding error: [%s]", respErr.Error())
			time.Sleep(5 * time.Second)
			continue
		}

		logger.Printf("respond response: [%s]", resp.GoString())
	}
}

func awsSubmitWorkflow() {
	svc := swf.New(sess)

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	id := fmt.Sprintf("test-%d",r1.Intn(1000000000))

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
		SetInput(`{ "test": "123" }`)

	res, startErr := svc.StartWorkflowExecution(startParams)

	if startErr != nil {
		logger.Printf("start error: [%s]",startErr.Error())
		return
	}

	logger.Printf("run id: [%s]", res.GoString())
}

func awsSubmitWorkflows() {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	for i := 0; i < 10; i++ {
		awsSubmitWorkflow()

		s := r1.Intn(45) + 15
		time.Sleep(time.Duration(s) * time.Second)
	}
}
