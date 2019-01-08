package main

import (
	"flag"
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

type configData struct {
	listenPort                configStringItem
	storageDir                configStringItem
	archiveDir                configStringItem
	iiifUrlTemplate           configStringItem
	concurrentUploads         configStringItem
	useHttps                  configBoolItem
	sslCrt                    configStringItem
	sslKey                    configStringItem
	tsApiHost                 configStringItem
	tsApiGetPidTemplate       configStringItem
	tsApiGetManifestTemplate  configStringItem
	tsApiGetFullTextTemplate  configStringItem
	tsApiPostFullTextTemplate configStringItem
	awsAccessKeyId            configStringItem
	awsSecretAccessKey        configStringItem
	awsRegion                 configStringItem
	awsSwfDomain              configStringItem
	awsSwfTaskList            configStringItem
	awsSwfWorkflowType        configStringItem
	awsSwfWorkflowVersion     configStringItem
	awsSwfWorkflowTimeout     configStringItem
	awsSwfDecisionTimeout     configStringItem
	awsLambdaFunction         configStringItem
	awsLambdaRole             configStringItem
	awsBucketName             configStringItem
}

var config configData

func init() {
	config.listenPort = configStringItem{value: "", configItem: configItem{flag: "l", env: "OCRWS_LISTEN_PORT", desc: "listen port"}}
	config.storageDir = configStringItem{value: "", configItem: configItem{flag: "t", env: "OCRWS_OCR_STORAGE_DIR", desc: "ocr storage directory"}}
	config.archiveDir = configStringItem{value: "", configItem: configItem{flag: "a", env: "OCRWS_OCR_ARCHIVE_DIR", desc: "ocr archive directory"}}
	config.iiifUrlTemplate = configStringItem{value: "", configItem: configItem{flag: "i", env: "OCRWS_IIIF_URL_TEMPLATE", desc: "iiif url template"}}
	config.concurrentUploads = configStringItem{value: "", configItem: configItem{flag: "o", env: "OCRWS_CONCURRENT_UPLOADS", desc: "concurrent uploads (0 => # cpu cores)"}}
	config.useHttps = configBoolItem{value: false, configItem: configItem{flag: "s", env: "OCRWS_USE_HTTPS", desc: "use https"}}
	config.sslCrt = configStringItem{value: "", configItem: configItem{flag: "c", env: "OCRWS_SSL_CRT", desc: "ssl crt"}}
	config.sslKey = configStringItem{value: "", configItem: configItem{flag: "k", env: "OCRWS_SSL_KEY", desc: "ssl key"}}
	config.tsApiHost = configStringItem{value: "", configItem: configItem{flag: "h", env: "OCRWS_TRACKSYS_API_HOST", desc: "tracksys host"}}
	config.tsApiGetPidTemplate = configStringItem{value: "", configItem: configItem{flag: "p", env: "OCRWS_TRACKSYS_API_GET_PID_TEMPLATE", desc: "tracksys api get pid template"}}
	config.tsApiGetManifestTemplate = configStringItem{value: "", configItem: configItem{flag: "m", env: "OCRWS_TRACKSYS_API_GET_MANIFEST_TEMPLATE", desc: "tracksys api get manifest template"}}
	config.tsApiGetFullTextTemplate = configStringItem{value: "", configItem: configItem{flag: "f", env: "OCRWS_TRACKSYS_API_GET_FULLTEXT_TEMPLATE", desc: "tracksys api get fulltext template"}}
	config.tsApiPostFullTextTemplate = configStringItem{value: "", configItem: configItem{flag: "u", env: "OCRWS_TRACKSYS_API_POST_FULLTEXT_TEMPLATE", desc: "tracksys api post fulltext template"}}
	config.awsAccessKeyId = configStringItem{value: "", configItem: configItem{flag: "A", env: "AWS_ACCESS_KEY_ID", desc: "aws access key id"}}
	config.awsSecretAccessKey = configStringItem{value: "", configItem: configItem{flag: "S", env: "AWS_SECRET_ACCESS_KEY", desc: "aws secret access key"}}
	config.awsRegion = configStringItem{value: "", configItem: configItem{flag: "R", env: "AWS_REGION", desc: "aws swf domain"}}
	config.awsSwfDomain = configStringItem{value: "", configItem: configItem{flag: "D", env: "AWS_SWF_DOMAIN", desc: "aws region"}}
	config.awsSwfTaskList = configStringItem{value: "", configItem: configItem{flag: "T", env: "AWS_SWF_TASKLIST", desc: "aws swf task list"}}
	config.awsSwfWorkflowType = configStringItem{value: "", configItem: configItem{flag: "W", env: "AWS_SWF_WORKFLOW_TYPE", desc: "aws swf workflow type"}}
	config.awsSwfWorkflowVersion = configStringItem{value: "", configItem: configItem{flag: "V", env: "AWS_SWF_WORKFLOW_VERSION", desc: "aws swf workflow version"}}
	config.awsSwfWorkflowTimeout = configStringItem{value: "", configItem: configItem{flag: "O", env: "AWS_SWF_WORKFLOW_TIMEOUT", desc: "aws swf workflow timeout"}}
	config.awsSwfDecisionTimeout = configStringItem{value: "", configItem: configItem{flag: "E", env: "AWS_SWF_DECISION_TIMEOUT", desc: "aws swf decision timeout"}}
	config.awsLambdaFunction = configStringItem{value: "", configItem: configItem{flag: "F", env: "AWS_LAMBDA_FUNCTION", desc: "aws lambda function"}}
	config.awsLambdaRole = configStringItem{value: "", configItem: configItem{flag: "L", env: "AWS_LAMBDA_ROLE", desc: "aws lambda role"}}
	config.awsBucketName = configStringItem{value: "", configItem: configItem{flag: "B", env: "AWS_BUCKET_NAME", desc: "aws bucket name"}}
}

func getBoolEnv(optEnv string) bool {
	value, _ := strconv.ParseBool(os.Getenv(optEnv))

	return value
}

func ensureConfigStringSet(item *configStringItem) bool {
	isSet := true

	if item.value == "" {
		isSet = false
		logger.Printf("[ERROR] %s is not set, use %s variable or -%s flag", item.desc, item.env, item.flag)
	}

	return isSet
}

func flagStringVar(item *configStringItem) {
	flag.StringVar(&item.value, item.flag, os.Getenv(item.env), item.desc)
}

func flagBoolVar(item *configBoolItem) {
	flag.BoolVar(&item.value, item.flag, getBoolEnv(item.env), item.desc)
}

func getConfigValues() {
	// get values from the command line first, falling back to environment variables
	flagStringVar(&config.listenPort)
	flagStringVar(&config.storageDir)
	flagStringVar(&config.archiveDir)
	flagStringVar(&config.iiifUrlTemplate)
	flagStringVar(&config.concurrentUploads)
	flagBoolVar(&config.useHttps)
	flagStringVar(&config.sslCrt)
	flagStringVar(&config.sslKey)
	flagStringVar(&config.tsApiHost)
	flagStringVar(&config.tsApiGetPidTemplate)
	flagStringVar(&config.tsApiGetManifestTemplate)
	flagStringVar(&config.tsApiGetFullTextTemplate)
	flagStringVar(&config.tsApiPostFullTextTemplate)
	flagStringVar(&config.awsAccessKeyId)
	flagStringVar(&config.awsSecretAccessKey)
	flagStringVar(&config.awsRegion)
	flagStringVar(&config.awsSwfDomain)
	flagStringVar(&config.awsSwfTaskList)
	flagStringVar(&config.awsSwfWorkflowType)
	flagStringVar(&config.awsSwfWorkflowVersion)
	flagStringVar(&config.awsSwfWorkflowTimeout)
	flagStringVar(&config.awsSwfDecisionTimeout)
	flagStringVar(&config.awsLambdaFunction)
	flagStringVar(&config.awsLambdaRole)
	flagStringVar(&config.awsBucketName)

	flag.Parse()

	// check each required option, displaying a warning for empty values.
	// die if any of them are not set
	configOK := true
	configOK = ensureConfigStringSet(&config.listenPort) && configOK
	configOK = ensureConfigStringSet(&config.storageDir) && configOK
	configOK = ensureConfigStringSet(&config.archiveDir) && configOK
	configOK = ensureConfigStringSet(&config.iiifUrlTemplate) && configOK
	configOK = ensureConfigStringSet(&config.concurrentUploads) && configOK
	configOK = ensureConfigStringSet(&config.tsApiHost) && configOK
	configOK = ensureConfigStringSet(&config.tsApiGetPidTemplate) && configOK
	configOK = ensureConfigStringSet(&config.tsApiGetManifestTemplate) && configOK
	configOK = ensureConfigStringSet(&config.tsApiGetFullTextTemplate) && configOK
	//configOK = ensureConfigStringSet(&config.tsApiPostFullTextTemplate) && configOK
	configOK = ensureConfigStringSet(&config.awsAccessKeyId) && configOK
	configOK = ensureConfigStringSet(&config.awsSecretAccessKey) && configOK
	configOK = ensureConfigStringSet(&config.awsRegion) && configOK
	configOK = ensureConfigStringSet(&config.awsSwfDomain) && configOK
	configOK = ensureConfigStringSet(&config.awsSwfTaskList) && configOK
	configOK = ensureConfigStringSet(&config.awsSwfWorkflowType) && configOK
	configOK = ensureConfigStringSet(&config.awsSwfWorkflowVersion) && configOK
	configOK = ensureConfigStringSet(&config.awsSwfWorkflowTimeout) && configOK
	configOK = ensureConfigStringSet(&config.awsSwfDecisionTimeout) && configOK
	configOK = ensureConfigStringSet(&config.awsLambdaFunction) && configOK
	configOK = ensureConfigStringSet(&config.awsLambdaRole) && configOK
	configOK = ensureConfigStringSet(&config.awsBucketName) && configOK

	if config.useHttps.value == true {
		configOK = ensureConfigStringSet(&config.sslCrt) && configOK
		configOK = ensureConfigStringSet(&config.sslKey) && configOK
	}

	if configOK == false {
		flag.Usage()
		os.Exit(1)
	}

	logger.Printf("[CONFIG] listenPort                = [%s]", config.listenPort.value)
	logger.Printf("[CONFIG] storageDir                = [%s]", config.storageDir.value)
	logger.Printf("[CONFIG] archiveDir                = [%s]", config.archiveDir.value)
	logger.Printf("[CONFIG] tsApiHost                 = [%s]", config.tsApiHost.value)
	logger.Printf("[CONFIG] tsApiGetPidTemplate       = [%s]", config.tsApiGetPidTemplate.value)
	logger.Printf("[CONFIG] tsApiGetManifestTemplate  = [%s]", config.tsApiGetManifestTemplate.value)
	logger.Printf("[CONFIG] tsApiGetFullTextTemplate  = [%s]", config.tsApiGetFullTextTemplate.value)
	logger.Printf("[CONFIG] tsApiPostFullTextTemplate = [%s]", config.tsApiPostFullTextTemplate.value)
	logger.Printf("[CONFIG] iiifUrlTemplate           = [%s]", config.iiifUrlTemplate.value)
	logger.Printf("[CONFIG] concurrentUploads         = [%s]", config.concurrentUploads.value)
	logger.Printf("[CONFIG] useHttps                  = [%s]", strconv.FormatBool(config.useHttps.value))
	logger.Printf("[CONFIG] sslCrt                    = [%s]", config.sslCrt.value)
	logger.Printf("[CONFIG] sslKey                    = [%s]", config.sslKey.value)
	logger.Printf("[CONFIG] awsAccessKeyId            = [%s]", config.awsAccessKeyId.value)
	logger.Printf("[CONFIG] awsSecretAccessKey        = [REDACTED]")
	logger.Printf("[CONFIG] awsRegion                 = [%s]", config.awsRegion.value)
	logger.Printf("[CONFIG] awsSwfDomain              = [%s]", config.awsSwfDomain.value)
	logger.Printf("[CONFIG] awsSwfTaskList            = [%s]", config.awsSwfTaskList.value)
	logger.Printf("[CONFIG] awsSwfWorkflowType        = [%s]", config.awsSwfWorkflowType.value)
	logger.Printf("[CONFIG] awsSwfWorkflowVersion     = [%s]", config.awsSwfWorkflowVersion.value)
	logger.Printf("[CONFIG] awsSwfWorkflowTimeout     = [%s]", config.awsSwfWorkflowTimeout.value)
	logger.Printf("[CONFIG] awsSwfDecisionTimeout     = [%s]", config.awsSwfDecisionTimeout.value)
	logger.Printf("[CONFIG] awsLambdaFunction         = [%s]", config.awsLambdaFunction.value)
	logger.Printf("[CONFIG] awsLambdaRole             = [%s]", config.awsLambdaRole.value)
	logger.Printf("[CONFIG] awsBucketName             = [%s]", config.awsBucketName.value)
}
