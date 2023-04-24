package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/vars"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

const (
	ParamCfgFileName         = "cfg"
	ParamCfgFileDefaultValue = ""

	ParamLogLevel             = "log-level"
	ParamLogLevelDefaultValue = -1

	ParamLksFileName             = "lks-file"
	ParamLksFileNameDefaultValue = "lks-sample.yml"

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
		log.Info().Int("[i]", i).Str("cmd", op.Cmd).Msg(logContext)
		log.Info().Int("[i]", i).Str("cnt", op.Container).Msg(logContext)
		log.Info().Int("[i]", i).Int("concurrency-level", op.ConcurrencyLevel).Msg(logContext)
		log.Info().Int("[i]", i).Str("context-query", op.CtxQueryText).Msg(logContext)
		log.Info().Int("[i]", i).Bool("delete-flag", op.DeleteFlag).Msg(logContext)
		log.Info().Int("[i]", i).Int("limit", op.Limit).Msg(logContext)
		log.Info().Int("[i]", i).Str("out", op.OutFile).Msg(logContext)
		log.Info().Int("[i]", i).Int("page-size", op.PageSize).Msg(logContext)
		log.Info().Int("[i]", i).Str("print", op.PrintTemplate).Msg(logContext)
		log.Info().Int("[i]", i).Str("query", op.QueryText).Msg(logContext)
	}
}

func ParseCmdLineArgs() (CmdLineArgs, error) {

	var err error

	args := CmdLineArgs{}

	argsFileNamePtr := flag.String(ParamCfgFileName, "", "yaml file of command args")
	lksFileNamePtr := flag.String(ParamLksFileName, "", "yaml file of cosmos config")
	deleteFlagPtr := flag.Bool(ParamDeleteFlag, false, "option to delete queried docs")
	concurrencyLevelPtr := flag.Int(ParamConcurrencyLevel, 0, "level of concurrency in modify ops")
	logLevelNamePtr := flag.Int(ParamLogLevel, 0, "log level to be used")
	pageSizePtr := flag.Int(ParamPageSize, 0, "page size used in the paged select ops")
	limitPtr := flag.Int(ParamLimit, 0, "limit the number of records returned")
	brokerPtr := flag.String(ParamBrokerName, "", "cosmos instance config name")
	dbPtr := flag.String(ParamDbName, "", "db name")
	collectionPtr := flag.String(ParamCollectionName, "", "container name")
	cmdPtr := flag.String(ParamCmd, "", fmt.Sprintf("cmd: %s, %s, %s", CmdSelect, CmdUpsert, CmdDelete))
	queryTextPtr := flag.String(ParamQuery, "", "cosmos query statement")
	ctxQueryTextPtr := flag.String(ParamContextQuery, "", "cosmos context query statement to get values for the actual target query")
	queryPrintTemplatePtr := flag.String(ParamPrintTemplate, "", "cosmos query statement")
	outFilePtr := flag.String(ParamOutFile, "", "output-file")
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
