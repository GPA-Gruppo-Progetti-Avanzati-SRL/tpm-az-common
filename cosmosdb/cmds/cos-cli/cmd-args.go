package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/vars"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
	"strings"
)

const (
	ParamCfgFileName         = "cfg"
	ParamCfgFileDefaultValue = ""

	ParamLogLevel             = "log-level"
	ParamLogLevelDefaultValue = -1

	ParamLksFileName             = "lks-file"
	ParamLksFileNameDefaultValue = "lks-cfg.yml"

	ParamBrokerName             = "cos"
	ParamBrokerNameDefaultValue = "default"

	ParamDbName             = "db"
	ParamDbNameDefaultValue = ""

	ParamCollectionName             = "cnt"
	ParamCollectionNameDefaultValue = ""

	ParamCmd             = "cmd"
	ParamCmdDefaultValue = CmdSelect

	ParamQuery             = "query"
	ParamQueryDefaultValue = "select * from c"

	ParamContextQuery             = "context-query"
	ParamContextQueryDefaultValue = ""

	ParamPrintTemplate             = "print"
	ParamPrintTemplateDefaultValue = "{{ .id }}:{{ .id }}:{{ .json }}"

	ParamOutFile             = "out"
	ParamOutFileDefaultValue = "cos-cli.out"

	ParamDeleteFlag             = "delete"
	ParamDeleteFlagDefaultValue = false

	ParamConcurrencyLevel             = "concurrency-level"
	ParamConcurrencyLevelDefaultValue = 1

	ParamPageSize             = "page-size"
	ParamPageSizeDefaultValue = 500

	ParamLimit             = "limit"
	ParamLimitDefaultValue = 0

	ParamTitle             = "title"
	ParamTitleDefaultValue = ""

	CmdSelect       = "select"
	CmdSelectDelete = "select-delete"
	CmdUpsert       = "upsert"
	CmdDelete       = "delete"
)

var commands = []string{CmdSelect, CmdDelete, CmdUpsert}

var defaultArgs = CmdLineArgs{
	LksFileName: ParamLksFileNameDefaultValue,
	Broker:      ParamBrokerNameDefaultValue,
	Db:          ParamDbNameDefaultValue,
	LogLevel:    ParamLogLevelDefaultValue,
	Operations: []CmdLineArgOperation{
		{
			Container:        ParamCollectionNameDefaultValue,
			Cmd:              ParamCmdDefaultValue,
			QueryText:        ParamQueryDefaultValue,
			CtxQueryText:     ParamContextQueryDefaultValue,
			PrintTemplate:    ParamPrintTemplateDefaultValue,
			OutFile:          ParamOutFileDefaultValue,
			DeleteFlag:       ParamDeleteFlagDefaultValue,
			ConcurrencyLevel: ParamConcurrencyLevelDefaultValue,
			PageSize:         ParamPageSizeDefaultValue,
			Limit:            ParamLimitDefaultValue,
		},
	},
}

type CmdLineArgOperation struct {
	Container        string `yaml:"cnt,omitempty" mapstructure:"cnt,omitempty" json:"cnt,omitempty"`
	Cmd              string `yaml:"cmd,omitempty" mapstructure:"cmd,omitempty" json:"cmd,omitempty"`
	Title            string `yaml:"title,omitempty" mapstructure:"title,omitempty" json:"title,omitempty"`
	QueryText        string `yaml:"query,omitempty" mapstructure:"query,omitempty" json:"query,omitempty"`
	CtxQueryText     string `yaml:"context-query,omitempty" mapstructure:"context-query,omitempty" json:"context-query,omitempty"`
	PrintTemplate    string `yaml:"print,omitempty" mapstructure:"print,omitempty" json:"print,omitempty"`
	OutFile          string `yaml:"out,omitempty" mapstructure:"out,omitempty" json:"out,omitempty"`
	DeleteFlag       bool   `yaml:"delete,omitempty" mapstructure:"delete,omitempty" json:"delete,omitempty"`
	ConcurrencyLevel int    `yaml:"concurrency-level,omitempty" mapstructure:"concurrency-level,omitempty" json:"concurrency-level,omitempty"`
	PageSize         int    `yaml:"page-size,omitempty" mapstructure:"page-size,omitempty" json:"page-size,omitempty"`
	Limit            int    `yaml:"limit,omitempty" mapstructure:"limit,omitempty" json:"limit,omitempty"`
}

type CmdLineArgs struct {
	LksConfig   *coslks.Config        `yaml:"-,omitempty" mapstructure:"-,omitempty" json:"-,omitempty"`
	LksFileName string                `yaml:"lks-file,omitempty" mapstructure:"lksFile,omitempty" json:"lksFile,omitempty"`
	Broker      string                `yaml:"cos,omitempty" mapstructure:"cos,omitempty" json:"cos,omitempty"`
	Db          string                `yaml:"db,omitempty" mapstructure:"db,omitempty" json:"db,omitempty"`
	LogLevel    int                   `yaml:"log-level,omitempty" mapstructure:"log-level,omitempty" json:"log-level,omitempty"`
	Operations  []CmdLineArgOperation `yaml:"ops,omitempty" mapstructure:"ops,omitempty" json:"ops,omitempty"`

	//Container        string         `yaml:"cnt,omitempty" mapstructure:"cnt,omitempty" json:"cnt,omitempty"`
	//Cmd              string         `yaml:"cmd,omitempty" mapstructure:"cmd,omitempty" json:"cmd,omitempty"`
	//QueryText        string         `yaml:"query,omitempty" mapstructure:"query,omitempty" json:"query,omitempty"`
	//PrintTemplate    string         `yaml:"print,omitempty" mapstructure:"print,omitempty" json:"print,omitempty"`
	//OutFile          string         `yaml:"out,omitempty" mapstructure:"out,omitempty" json:"out,omitempty"`
	//LogLevel         int            `yaml:"log-level,omitempty" mapstructure:"log-level,omitempty" json:"log-level,omitempty"`
	//DeleteFlag       bool           `yaml:"delete,omitempty" mapstructure:"delete,omitempty" json:"delete,omitempty"`
	//ConcurrencyLevel int            `yaml:"concurrency-level,omitempty" mapstructure:"concurrency-level,omitempty" json:"concurrency-level,omitempty"`
	//PageSize         int            `yaml:"page-size,omitempty" mapstructure:"page-size,omitempty" json:"page-size,omitempty"`
}

func (args *CmdLineArgs) Log(logContext string) {

	log.Info().Str("cos", args.Broker).Msg(logContext)
	log.Info().Str("db", args.Db).Msg(logContext)
	log.Info().Str("lks-file", args.LksFileName).Msg(logContext)
	log.Info().Int("log-level", args.LogLevel).Msg(logContext)

	for i, op := range args.Operations {

		evt := log.Info().Int("[i]", i)
		switch op.Cmd {
		case CmdSelect:
			evt.Str(ParamCmd, op.Cmd)
			evt.Str(ParamCollectionName, op.Container)
			evt.Str(ParamContextQuery, op.CtxQueryText)
			evt.Int(ParamLimit, op.Limit)
			evt.Str(ParamOutFile, op.OutFile)
			evt.Int(ParamPageSize, op.PageSize)
			evt.Str(ParamPrintTemplate, op.PrintTemplate)
			evt.Str(ParamQuery, op.QueryText)
		case CmdSelectDelete:
			evt.Str(ParamCmd, op.Cmd)
			evt.Str(ParamCollectionName, op.Container)
			evt.Str(ParamContextQuery, op.CtxQueryText)
			evt.Int(ParamLimit, op.Limit)
			evt.Int(ParamPageSize, op.PageSize)
			evt.Str(ParamQuery, op.QueryText)
			evt.Int(ParamConcurrencyLevel, op.ConcurrencyLevel)
			evt.Bool(ParamDeleteFlag, op.DeleteFlag)
		}

		evt.Msg(logContext)
	}
}

func (op *CmdLineArgOperation) String() string {

	var sb strings.Builder
	switch op.Cmd {
	case CmdSelect:
		sb.WriteString(fmt.Sprintf("-%s %s ", ParamCmd, op.Cmd))
		sb.WriteString(op.StringParam(ParamCollectionName, op.Container, ParamCollectionNameDefaultValue))
		sb.WriteString(op.StringParam(ParamQuery, op.QueryText, ParamQueryDefaultValue))
		sb.WriteString(op.StringParam(ParamPrintTemplate, op.PrintTemplate, ParamPrintTemplateDefaultValue))
		sb.WriteString(op.StringParam(ParamContextQuery, op.CtxQueryText, ParamContextQueryDefaultValue))
		sb.WriteString(op.intParam2String(ParamLimit, op.Limit, ParamLimitDefaultValue))
		sb.WriteString(op.intParam2String(ParamPageSize, op.PageSize, ParamPageSizeDefaultValue))
		sb.WriteString(op.StringParam(ParamOutFile, op.OutFile, ParamOutFileDefaultValue))
	case CmdSelectDelete:
		sb.WriteString(fmt.Sprintf("-%s %s ", ParamCmd, CmdSelect))
		sb.WriteString(fmt.Sprintf("-%s", ParamDeleteFlag))
		sb.WriteString(op.StringParam(ParamCollectionName, op.Container, ParamCollectionNameDefaultValue))
		sb.WriteString(op.StringParam(ParamQuery, op.QueryText, ParamQueryDefaultValue))
		sb.WriteString(op.StringParam(ParamContextQuery, op.CtxQueryText, ParamContextQueryDefaultValue))
		sb.WriteString(op.intParam2String(ParamLimit, op.Limit, ParamLimitDefaultValue))
		sb.WriteString(op.intParam2String(ParamPageSize, op.PageSize, ParamPageSizeDefaultValue))
		sb.WriteString(op.intParam2String(ParamConcurrencyLevel, op.ConcurrencyLevel, ParamConcurrencyLevelDefaultValue))
	}

	return sb.String()
}

func (op *CmdLineArgOperation) StringParam(name, val, defaultValue string) string {
	if val != defaultValue {
		s, _ := json.Marshal(val)
		return fmt.Sprintf("-%s %s ", name, s)
	}

	return ""
}

func (op *CmdLineArgOperation) intParam2String(name string, val, defaultValue int) string {
	if val != defaultValue {
		return fmt.Sprintf("-%s %d ", name, val)
	}

	return ""
}

func ParseCmdLineArgs() (CmdLineArgs, error) {

	var err error

	args := CmdLineArgs{}

	argsFileNamePtr := flag.String(ParamCfgFileName, "", fmt.Sprintf("yaml file of command args (default: %s)", ParamCfgFileDefaultValue))
	lksFileNamePtr := flag.String(ParamLksFileName, "", fmt.Sprintf("yaml file of cosmos config (connection string and optionally db and collection resolution) (default: %s)", ParamLksFileNameDefaultValue))
	deleteFlagPtr := flag.Bool(ParamDeleteFlag, false, fmt.Sprintf("option to delete queried docs  (default: %t)", false))
	concurrencyLevelPtr := flag.Int(ParamConcurrencyLevel, 0, fmt.Sprintf("level of concurrency in modify ops  (default: %d)", ParamConcurrencyLevelDefaultValue))
	logLevelNamePtr := flag.Int(ParamLogLevel, 0, fmt.Sprintf("log level to be used (default: %d)", ParamLogLevelDefaultValue))
	pageSizePtr := flag.Int(ParamPageSize, 0, fmt.Sprintf("page size used in the paged select ops (default: %d)", ParamPageSizeDefaultValue))
	limitPtr := flag.Int(ParamLimit, 0, fmt.Sprintf("limit the number of records returned (default: %d)", ParamLimitDefaultValue))
	brokerPtr := flag.String(ParamBrokerName, "", fmt.Sprintf("cosmos instance config name (default: %s)", ParamBrokerNameDefaultValue))
	dbPtr := flag.String(ParamDbName, "", fmt.Sprintf("db name or id (resolved by the lks file) (default: %s)", ParamDbNameDefaultValue))
	collectionPtr := flag.String(ParamCollectionName, "", fmt.Sprintf("container name or id (resolved by the lks file) (default: %s)", ParamCollectionNameDefaultValue))
	cmdPtr := flag.String(ParamCmd, "", fmt.Sprintf("cmd: %s, %s, %s (default: %s)", CmdSelect, CmdUpsert, CmdDelete, ParamCmdDefaultValue))
	queryTextPtr := flag.String(ParamQuery, "", fmt.Sprintf("cosmos query statement (default: %s)", ParamQueryDefaultValue))
	ctxQueryTextPtr := flag.String(ParamContextQuery, "", fmt.Sprintf("cosmos context query statement to get values for the actual target query (default: %s)", ParamContextQueryDefaultValue))
	queryPrintTemplatePtr := flag.String(ParamPrintTemplate, "", fmt.Sprintf("cosmos print template for queried records (default: %s)", ParamPrintTemplateDefaultValue))
	outFilePtr := flag.String(ParamOutFile, "", fmt.Sprintf("output-file (default: %s)", ParamOutFileDefaultValue))
	flag.Parse()

	if *argsFileNamePtr != "" {
		args, err = readArgsFile(*argsFileNamePtr)
		if err != nil {
			return args, err
		}
	}

	args.Broker = util.StringCoalesce(*brokerPtr, args.Broker, defaultArgs.Broker)
	args.Db = util.StringCoalesce(*dbPtr, args.Db, defaultArgs.Db)
	args.LksFileName = util.StringCoalesce(*lksFileNamePtr, args.LksFileName, defaultArgs.LksFileName)
	args.LogLevel = util.IntCoalesce(*logLevelNamePtr, args.LogLevel, defaultArgs.LogLevel)

	if len(args.Operations) == 0 {
		args.Operations = []CmdLineArgOperation{
			{
				Container:        util.StringCoalesce(*collectionPtr, defaultArgs.Operations[0].Container),
				Cmd:              util.StringCoalesce(*cmdPtr, defaultArgs.Operations[0].Cmd),
				QueryText:        util.StringCoalesce(*queryTextPtr, defaultArgs.Operations[0].QueryText),
				CtxQueryText:     util.StringCoalesce(*ctxQueryTextPtr, defaultArgs.Operations[0].CtxQueryText),
				PrintTemplate:    util.StringCoalesce(*queryPrintTemplatePtr, defaultArgs.Operations[0].PrintTemplate),
				OutFile:          util.StringCoalesce(*outFilePtr, defaultArgs.Operations[0].OutFile),
				DeleteFlag:       *deleteFlagPtr,
				ConcurrencyLevel: util.IntCoalesce(*concurrencyLevelPtr, defaultArgs.Operations[0].ConcurrencyLevel),
				PageSize:         util.IntCoalesce(*pageSizePtr, defaultArgs.Operations[0].PageSize),
				Limit:            util.IntCoalesce(*limitPtr, defaultArgs.Operations[0].Limit),
			},
		}
	} else {
		for i := range args.Operations {
			args.Operations[i].Container = util.StringCoalesce(*collectionPtr, args.Operations[i].Container, defaultArgs.Operations[0].Container)
			args.Operations[i].Cmd = util.StringCoalesce(*cmdPtr, args.Operations[i].Cmd, defaultArgs.Operations[0].Cmd)
			args.Operations[i].QueryText = util.StringCoalesce(*queryTextPtr, args.Operations[i].QueryText, defaultArgs.Operations[0].QueryText)
			args.Operations[i].CtxQueryText = util.StringCoalesce(*ctxQueryTextPtr, args.Operations[i].CtxQueryText, defaultArgs.Operations[0].CtxQueryText)
			args.Operations[i].PrintTemplate = util.StringCoalesce(*queryPrintTemplatePtr, args.Operations[i].PrintTemplate, defaultArgs.Operations[0].PrintTemplate)
			args.Operations[i].OutFile = util.StringCoalesce(*outFilePtr, args.Operations[i].OutFile, defaultArgs.Operations[0].OutFile)
			args.Operations[i].ConcurrencyLevel = util.IntCoalesce(*concurrencyLevelPtr, args.Operations[i].ConcurrencyLevel, defaultArgs.Operations[0].ConcurrencyLevel)
			args.Operations[i].PageSize = util.IntCoalesce(*pageSizePtr, args.Operations[i].PageSize, defaultArgs.Operations[0].PageSize)
			args.Operations[i].Limit = util.IntCoalesce(*limitPtr, args.Operations[i].Limit, defaultArgs.Operations[0].Limit)
			if *deleteFlagPtr {
				args.Operations[i].DeleteFlag = *deleteFlagPtr
			}
		}
	}

	for i, op := range args.Operations {
		if op.Cmd == "" || !valueIn(op.Cmd, commands) {
			flag.Usage()
			return args, fmt.Errorf("missing or invalid command parameter: %s", op.Cmd)
		}

		switch op.Cmd {
		case CmdSelect:
			if cfg, err := validateCosmosParams(args.LksFileName, args.Broker, args.Db, op.Container); err != nil {
				flag.Usage()
				return args, err
			} else {
				args.LksConfig = cfg
				cnt := cfg.GetCollectionNameById(op.Container)
				if cnt != "" {
					args.Operations[i].Container = cnt
				}

				db := cfg.GetDbNameById(args.Db)
				if db != "" {
					args.Db = db
				}
			}

			if op.QueryText == "" {
				flag.Usage()
				return args, fmt.Errorf("missing select statement")
			}

			vs, err := varResolver.FindVariableReferences(op.QueryText, varResolver.DollarVariableReference)
			if err != nil {
				flag.Usage()
				return args, fmt.Errorf("missing select statement")
			}

			if len(vs) > 0 {
				if op.CtxQueryText == "" {
					flag.Usage()
					return args, fmt.Errorf("context query Not specified but variable refs found in query text")
				}
			} else {
				if op.CtxQueryText != "" {
					flag.Usage()
					return args, fmt.Errorf("context query specified but No variable refs found in query text")
				}
			}

			if op.DeleteFlag {
				args.Operations[i].Cmd = CmdSelectDelete
			}
		default:
			flag.Usage()
			return args, fmt.Errorf("to be implemented command: %s", op.Cmd)
		}

	}

	zerolog.SetGlobalLevel(zerolog.Level(args.LogLevel))
	return args, nil
}

func valueIn(s string, values []string) bool {
	for _, v := range values {
		if s == v {
			return true
		}
	}

	return false
}

func validateCosmosParams(cfgFileName, cosmosName, db, cnt string) (*coslks.Config, error) {
	if cosmosName == "" {
		return nil, errors.New("cosmos instance config name not specified")
	}

	if db == "" {
		return nil, errors.New("db name not specified")
	}

	if cnt == "" {
		return nil, errors.New("container name not specified")
	}

	if cfgFileName == "" {
		return nil, errors.New("cosmos config name not specified")
	}

	if !util.FileExists(cfgFileName) {
		return nil, fmt.Errorf("the cosmos config file %s cannot be found", cfgFileName)
	}

	b, err := util.ReadFileAndResolveEnvVars(cfgFileName)
	if err != nil {
		return nil, fmt.Errorf("error reading the cosmos config file %s", cfgFileName)
	}

	cosmosCfg := coslks.Config{}
	err = yaml.Unmarshal(b, &cosmosCfg)
	if err != nil {
		return nil, err
	}

	return &cosmosCfg, nil
}

func readArgsFile(argsFile string) (CmdLineArgs, error) {

	m := CmdLineArgs{}

	if !util.FileExists(argsFile) {
		return m, fmt.Errorf("the cos-cli arguments file %s cannot be found", argsFile)
	}

	b, err := util.ReadFileAndResolveEnvVars(argsFile)
	if err != nil {
		return m, fmt.Errorf("error reading the cos-cli arguments file %s", argsFile)
	}

	err = yaml.Unmarshal(b, &m)
	if err != nil {
		return m, err
	}

	return m, nil
}
