package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
)

type configItem struct {
	flag string
	env  string
	desc string
}

type configStringItem struct {
	value string
	configItem
}

type configBoolItem struct {
	value bool
	configItem
}

type configIntItem struct {
	value int
	configItem
}

type configData struct {
	listenPort            configStringItem
	storageDir            configStringItem
	archiveDir            configStringItem
	lambdaAttempts        configStringItem
	concurrentUploads     configStringItem
	iiifURLTemplate       configStringItem
	tsAPIHost             configStringItem
	tsAPIKey              configStringItem
	tsReadOnly            configBoolItem
	emailName             configStringItem
	emailAddress          configStringItem
	emailHost             configStringItem
	emailPort             configIntItem
	awsDisabled           configBoolItem
	awsAccessKeyID        configStringItem
	awsSecretAccessKey    configStringItem
	awsRegion             configStringItem
	awsSwfDomain          configStringItem
	awsSwfTaskList        configStringItem
	awsSwfWorkflowType    configStringItem
	awsSwfWorkflowVersion configStringItem
	awsSwfWorkflowTimeout configStringItem
	awsSwfDecisionTimeout configStringItem
	awsLambdaFunction     configStringItem
	awsLambdaTimeout      configStringItem
	awsBucketName         configStringItem
}

var config configData

func init() {
	config.listenPort = configStringItem{value: "", configItem: configItem{flag: "l", env: "OCRWS_LISTEN_PORT", desc: "listen port"}}
	config.storageDir = configStringItem{value: "", configItem: configItem{flag: "t", env: "OCRWS_OCR_STORAGE_DIR", desc: "ocr storage directory"}}
	config.archiveDir = configStringItem{value: "", configItem: configItem{flag: "a", env: "OCRWS_OCR_ARCHIVE_DIR", desc: "ocr archive directory"}}
	config.lambdaAttempts = configStringItem{value: "", configItem: configItem{flag: "e", env: "OCRWS_LAMBDA_ATTEMPTS", desc: "max lambda attempts"}}
	config.concurrentUploads = configStringItem{value: "", configItem: configItem{flag: "o", env: "OCRWS_CONCURRENT_UPLOADS", desc: "concurrent uploads (0 => # cpu cores)"}}
	config.iiifURLTemplate = configStringItem{value: "", configItem: configItem{flag: "i", env: "OCRWS_IIIF_URL_TEMPLATE", desc: "iiif url template"}}
	config.tsAPIHost = configStringItem{value: "", configItem: configItem{flag: "h", env: "OCRWS_TRACKSYS_API_HOST", desc: "tracksys host"}}
	config.tsAPIKey = configStringItem{value: "", configItem: configItem{flag: "k", env: "OCRWS_TRACKSYS_API_KEY", desc: "tracksys write key"}}
	config.tsReadOnly = configBoolItem{value: false, configItem: configItem{flag: "r", env: "OCRWS_TRACKSYS_READ_ONLY", desc: "tracksys read-only flag"}}
	config.emailName = configStringItem{value: "", configItem: configItem{flag: "n", env: "OCRWS_EMAIL_NAME", desc: "email name"}}
	config.emailAddress = configStringItem{value: "", configItem: configItem{flag: "d", env: "OCRWS_EMAIL_ADDRESS", desc: "email address"}}
	config.emailHost = configStringItem{value: "", configItem: configItem{flag: "s", env: "OCRWS_EMAIL_HOST", desc: "smtp host"}}
	config.emailPort = configIntItem{value: 0, configItem: configItem{flag: "p", env: "OCRWS_EMAIL_PORT", desc: "smtp port"}}
	config.awsDisabled = configBoolItem{value: false, configItem: configItem{flag: "L", env: "AWS_DISABLED", desc: "aws disabled flag"}}
	config.awsAccessKeyID = configStringItem{value: "", configItem: configItem{flag: "A", env: "AWS_ACCESS_KEY_ID", desc: "aws access key id"}}
	config.awsSecretAccessKey = configStringItem{value: "", configItem: configItem{flag: "S", env: "AWS_SECRET_ACCESS_KEY", desc: "aws secret access key"}}
	config.awsRegion = configStringItem{value: "", configItem: configItem{flag: "R", env: "AWS_REGION", desc: "aws swf domain"}}
	config.awsSwfDomain = configStringItem{value: "", configItem: configItem{flag: "D", env: "AWS_SWF_DOMAIN", desc: "aws region"}}
	config.awsSwfTaskList = configStringItem{value: "", configItem: configItem{flag: "T", env: "AWS_SWF_TASKLIST", desc: "aws swf task list"}}
	config.awsSwfWorkflowType = configStringItem{value: "", configItem: configItem{flag: "W", env: "AWS_SWF_WORKFLOW_TYPE", desc: "aws swf workflow type"}}
	config.awsSwfWorkflowVersion = configStringItem{value: "", configItem: configItem{flag: "V", env: "AWS_SWF_WORKFLOW_VERSION", desc: "aws swf workflow version"}}
	config.awsSwfWorkflowTimeout = configStringItem{value: "", configItem: configItem{flag: "O", env: "AWS_SWF_WORKFLOW_TIMEOUT", desc: "aws swf workflow timeout"}}
	config.awsSwfDecisionTimeout = configStringItem{value: "", configItem: configItem{flag: "E", env: "AWS_SWF_DECISION_TIMEOUT", desc: "aws swf decision timeout"}}
	config.awsLambdaFunction = configStringItem{value: "", configItem: configItem{flag: "F", env: "AWS_LAMBDA_FUNCTION", desc: "aws lambda function"}}
	config.awsLambdaTimeout = configStringItem{value: "", configItem: configItem{flag: "I", env: "AWS_LAMBDA_TIMEOUT", desc: "aws lambda timeout"}}
	config.awsBucketName = configStringItem{value: "", configItem: configItem{flag: "B", env: "AWS_BUCKET_NAME", desc: "aws bucket name"}}
}

func getBoolEnv(optEnv string) bool {
	value, _ := strconv.ParseBool(os.Getenv(optEnv))

	return value
}

func getIntEnv(optEnv string) int {
	value, _ := strconv.Atoi(os.Getenv(optEnv))

	return value
}

func ensureConfigStringSet(item *configStringItem) bool {
	if item.value == "" {
		log.Printf("ERROR: [CONFIG] %s is not set, use %s variable or -%s flag", item.desc, item.env, item.flag)
		return false
	}
	return true
}

func flagStringVar(item *configStringItem) {
	flag.StringVar(&item.value, item.flag, os.Getenv(item.env), item.desc)
}

func flagBoolVar(item *configBoolItem) {
	flag.BoolVar(&item.value, item.flag, getBoolEnv(item.env), item.desc)
}

func flagIntVar(item *configIntItem) {
	flag.IntVar(&item.value, item.flag, getIntEnv(item.env), item.desc)
}

func maskValue(value string) string {
	if len(value) < 8 {
		return "......."
	}

	return fmt.Sprintf("...%s", value[len(value)-4:])
}

func getConfigValues() {
	// get values from the command line first, falling back to environment variables
	flagStringVar(&config.listenPort)
	flagStringVar(&config.storageDir)
	flagStringVar(&config.archiveDir)
	flagStringVar(&config.lambdaAttempts)
	flagStringVar(&config.concurrentUploads)
	flagStringVar(&config.iiifURLTemplate)
	flagStringVar(&config.tsAPIHost)
	flagStringVar(&config.tsAPIKey)
	flagBoolVar(&config.tsReadOnly)
	flagStringVar(&config.emailName)
	flagStringVar(&config.emailAddress)
	flagStringVar(&config.emailHost)
	flagIntVar(&config.emailPort)
	flagBoolVar(&config.awsDisabled)
	flagStringVar(&config.awsAccessKeyID)
	flagStringVar(&config.awsSecretAccessKey)
	flagStringVar(&config.awsRegion)
	flagStringVar(&config.awsSwfDomain)
	flagStringVar(&config.awsSwfTaskList)
	flagStringVar(&config.awsSwfWorkflowType)
	flagStringVar(&config.awsSwfWorkflowVersion)
	flagStringVar(&config.awsSwfWorkflowTimeout)
	flagStringVar(&config.awsSwfDecisionTimeout)
	flagStringVar(&config.awsLambdaFunction)
	flagStringVar(&config.awsLambdaTimeout)
	flagStringVar(&config.awsBucketName)

	flag.Parse()

	// check each required option, displaying a warning for empty values.
	// die if any of them are not set
	configOK := true
	configOK = ensureConfigStringSet(&config.listenPort) && configOK
	configOK = ensureConfigStringSet(&config.storageDir) && configOK
	configOK = ensureConfigStringSet(&config.archiveDir) && configOK
	configOK = ensureConfigStringSet(&config.lambdaAttempts) && configOK
	configOK = ensureConfigStringSet(&config.concurrentUploads) && configOK
	configOK = ensureConfigStringSet(&config.iiifURLTemplate) && configOK
	configOK = ensureConfigStringSet(&config.tsAPIHost) && configOK
	configOK = ensureConfigStringSet(&config.tsAPIKey) && configOK
	configOK = ensureConfigStringSet(&config.emailName) && configOK
	configOK = ensureConfigStringSet(&config.emailAddress) && configOK
	configOK = ensureConfigStringSet(&config.emailHost) && configOK

	if config.awsDisabled.value == false {
		configOK = ensureConfigStringSet(&config.awsAccessKeyID) && configOK
		configOK = ensureConfigStringSet(&config.awsSecretAccessKey) && configOK
		configOK = ensureConfigStringSet(&config.awsRegion) && configOK
		configOK = ensureConfigStringSet(&config.awsSwfDomain) && configOK
		configOK = ensureConfigStringSet(&config.awsSwfTaskList) && configOK
		configOK = ensureConfigStringSet(&config.awsSwfWorkflowType) && configOK
		configOK = ensureConfigStringSet(&config.awsSwfWorkflowVersion) && configOK
		configOK = ensureConfigStringSet(&config.awsSwfWorkflowTimeout) && configOK
		configOK = ensureConfigStringSet(&config.awsSwfDecisionTimeout) && configOK
		configOK = ensureConfigStringSet(&config.awsLambdaFunction) && configOK
		configOK = ensureConfigStringSet(&config.awsLambdaTimeout) && configOK
		configOK = ensureConfigStringSet(&config.awsBucketName) && configOK
	}

	if configOK == false {
		flag.Usage()
		os.Exit(1)
	}

	log.Printf("[CONFIG] listenPort            = [%s]", config.listenPort.value)
	log.Printf("[CONFIG] storageDir            = [%s]", config.storageDir.value)
	log.Printf("[CONFIG] archiveDir            = [%s]", config.archiveDir.value)
	log.Printf("[CONFIG] lambdaAttempts        = [%s]", config.lambdaAttempts.value)
	log.Printf("[CONFIG] concurrentUploads     = [%s]", config.concurrentUploads.value)
	log.Printf("[CONFIG] iiifURLTemplate       = [%s]", config.iiifURLTemplate.value)
	log.Printf("[CONFIG] tsAPIHost             = [%s]", config.tsAPIHost.value)
	log.Printf("[CONFIG] tsReadOnly            = [%v]", config.tsReadOnly.value)
	log.Printf("[CONFIG] emailName             = [%s]", config.emailName.value)
	log.Printf("[CONFIG] emailAddress          = [%s]", config.emailAddress.value)
	log.Printf("[CONFIG] emailHost             = [%s]", config.emailHost.value)
	log.Printf("[CONFIG] emailPort             = [%d]", config.emailPort.value)
	log.Printf("[CONFIG] awsDisabled           = [%v]", config.awsDisabled.value)
	log.Printf("[CONFIG] awsAccessKeyID        = [%s]", maskValue(config.awsAccessKeyID.value))
	log.Printf("[CONFIG] awsSecretAccessKey    = [%s]", maskValue(config.awsSecretAccessKey.value))
	log.Printf("[CONFIG] awsRegion             = [%s]", config.awsRegion.value)
	log.Printf("[CONFIG] awsSwfDomain          = [%s]", config.awsSwfDomain.value)
	log.Printf("[CONFIG] awsSwfTaskList        = [%s]", config.awsSwfTaskList.value)
	log.Printf("[CONFIG] awsSwfWorkflowType    = [%s]", config.awsSwfWorkflowType.value)
	log.Printf("[CONFIG] awsSwfWorkflowVersion = [%s]", config.awsSwfWorkflowVersion.value)
	log.Printf("[CONFIG] awsSwfWorkflowTimeout = [%s]", config.awsSwfWorkflowTimeout.value)
	log.Printf("[CONFIG] awsSwfDecisionTimeout = [%s]", config.awsSwfDecisionTimeout.value)
	log.Printf("[CONFIG] awsLambdaFunction     = [%s]", config.awsLambdaFunction.value)
	log.Printf("[CONFIG] awsLambdaTimeout      = [%s]", config.awsLambdaTimeout.value)
	log.Printf("[CONFIG] awsBucketName         = [%s]", config.awsBucketName.value)
}
