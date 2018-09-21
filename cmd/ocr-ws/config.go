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
	listenPort          configStringItem
	dbHost              configStringItem
	dbName              configStringItem
	dbUser              configStringItem
	dbPass              configStringItem
	dbAllowOldPasswords configBoolItem
	jp2kDir             configStringItem
	archiveDir          configStringItem
	storageDir          configStringItem
	templateDir         configStringItem
	scriptDir           configStringItem
	allowUnpublished    configBoolItem
	iiifUrlTemplate     configStringItem
	workerType          configStringItem
	workerCount         configStringItem
	useHttps            configBoolItem
	sslCrt              configStringItem
	sslKey              configStringItem
}

var config configData

func init() {
	config.listenPort = configStringItem{value: "", configItem: configItem{flag: "l", env: "OCRWS_LISTEN_PORT", desc: "listen port"}}
	config.dbHost = configStringItem{value: "", configItem: configItem{flag: "h", env: "OCRWS_DB_HOST", desc: "database host"}}
	config.dbName = configStringItem{value: "", configItem: configItem{flag: "n", env: "OCRWS_DB_NAME", desc: "database name"}}
	config.dbUser = configStringItem{value: "", configItem: configItem{flag: "u", env: "OCRWS_DB_USER", desc: "database user"}}
	config.dbPass = configStringItem{value: "", configItem: configItem{flag: "p", env: "OCRWS_DB_PASS", desc: "database password"}}
	config.dbAllowOldPasswords = configBoolItem{value: false, configItem: configItem{flag: "o", env: "OCRWS_DB_ALLOW_OLD_PASSWORDS", desc: "allow old database passwords"}}
	config.jp2kDir = configStringItem{value: "", configItem: configItem{flag: "j", env: "OCRWS_JP2K_DIR", desc: "jp2k directory"}}
	config.archiveDir = configStringItem{value: "", configItem: configItem{flag: "m", env: "OCRWS_ARCHIVE_DIR", desc: "archival tif mount directory"}}
	config.storageDir = configStringItem{value: "", configItem: configItem{flag: "t", env: "OCRWS_OCR_STORAGE_DIR", desc: "ocr storage directory"}}
	config.templateDir = configStringItem{value: "", configItem: configItem{flag: "w", env: "OCRWS_WEB_TEMPLATE_DIR", desc: "web template directory"}}
	config.scriptDir = configStringItem{value: "", configItem: configItem{flag: "r", env: "OCRWS_SCRIPT_DIR", desc: "helper script directory"}}
	config.allowUnpublished = configBoolItem{value: false, configItem: configItem{flag: "a", env: "OCRWS_ALLOW_UNPUBLISHED", desc: "allow unpublished"}}
	config.iiifUrlTemplate = configStringItem{value: "", configItem: configItem{flag: "i", env: "OCRWS_IIIF_URL_TEMPLATE", desc: "iiif url template"}}
	config.workerType = configStringItem{value: "", configItem: configItem{flag: "y", env: "OCRWS_WORKER_TYPE", desc: "worker type (tesseract/other APIs tbd)"}}
	config.workerCount = configStringItem{value: "", configItem: configItem{flag: "x", env: "OCRWS_WORKER_COUNT", desc: "number of workers in worker pool"}}
	config.useHttps = configBoolItem{value: false, configItem: configItem{flag: "s", env: "OCRWS_USE_HTTPS", desc: "use https"}}
	config.sslCrt = configStringItem{value: "", configItem: configItem{flag: "c", env: "OCRWS_SSL_CRT", desc: "ssl crt"}}
	config.sslKey = configStringItem{value: "", configItem: configItem{flag: "k", env: "OCRWS_SSL_KEY", desc: "ssl key"}}
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
	flagStringVar(&config.dbHost)
	flagStringVar(&config.dbName)
	flagStringVar(&config.dbUser)
	flagStringVar(&config.dbPass)
	flagBoolVar(&config.dbAllowOldPasswords)
	flagStringVar(&config.jp2kDir)
	flagStringVar(&config.archiveDir)
	flagStringVar(&config.storageDir)
	flagStringVar(&config.templateDir)
	flagStringVar(&config.scriptDir)
	flagBoolVar(&config.allowUnpublished)
	flagStringVar(&config.iiifUrlTemplate)
	flagStringVar(&config.workerType)
	flagStringVar(&config.workerCount)
	flagBoolVar(&config.useHttps)
	flagStringVar(&config.sslCrt)
	flagStringVar(&config.sslKey)

	flag.Parse()

	// check each required option, displaying a warning for empty values.
	// die if any of them are not set
	configOK := true
	configOK = ensureConfigStringSet(&config.listenPort) && configOK
	configOK = ensureConfigStringSet(&config.dbHost) && configOK
	configOK = ensureConfigStringSet(&config.dbName) && configOK
	configOK = ensureConfigStringSet(&config.dbUser) && configOK
	configOK = ensureConfigStringSet(&config.dbPass) && configOK
	configOK = ensureConfigStringSet(&config.jp2kDir) && configOK
	configOK = ensureConfigStringSet(&config.archiveDir) && configOK
	configOK = ensureConfigStringSet(&config.storageDir) && configOK
	configOK = ensureConfigStringSet(&config.templateDir) && configOK
	configOK = ensureConfigStringSet(&config.scriptDir) && configOK
	configOK = ensureConfigStringSet(&config.iiifUrlTemplate) && configOK
	configOK = ensureConfigStringSet(&config.workerType) && configOK
	configOK = ensureConfigStringSet(&config.workerCount) && configOK
	if config.useHttps.value == true {
		configOK = ensureConfigStringSet(&config.sslCrt) && configOK
		configOK = ensureConfigStringSet(&config.sslKey) && configOK
	}

	if configOK == false {
		flag.Usage()
		os.Exit(1)
	}

	logger.Printf("[CONFIG] listenPort          = [%s]", config.listenPort.value)
	logger.Printf("[CONFIG] dbHost              = [%s]", config.dbHost.value)
	logger.Printf("[CONFIG] dbName              = [%s]", config.dbName.value)
	logger.Printf("[CONFIG] dbUser              = [%s]", config.dbUser.value)
	logger.Printf("[CONFIG] dbPass              = [REDACTED]")
	logger.Printf("[CONFIG] dbAllowOldPasswords = [%s]", strconv.FormatBool(config.dbAllowOldPasswords.value))
	logger.Printf("[CONFIG] jp2kDir             = [%s]", config.jp2kDir.value)
	logger.Printf("[CONFIG] archiveDir          = [%s]", config.archiveDir.value)
	logger.Printf("[CONFIG] storageDir          = [%s]", config.storageDir.value)
	logger.Printf("[CONFIG] templateDir         = [%s]", config.templateDir.value)
	logger.Printf("[CONFIG] scriptDir           = [%s]", config.scriptDir.value)
	logger.Printf("[CONFIG] allowUnpublished    = [%s]", strconv.FormatBool(config.allowUnpublished.value))
	logger.Printf("[CONFIG] iiifUrlTemplate     = [%s]", config.iiifUrlTemplate.value)
	logger.Printf("[CONFIG] workerType          = [%s]", config.workerType.value)
	logger.Printf("[CONFIG] workerCount         = [%s]", config.workerCount.value)
	logger.Printf("[CONFIG] useHttps            = [%s]", strconv.FormatBool(config.useHttps.value))
	logger.Printf("[CONFIG] sslCrt              = [%s]", config.sslCrt.value)
	logger.Printf("[CONFIG] sslKey              = [%s]", config.sslKey.value)
}
