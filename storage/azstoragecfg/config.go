package azstoragecfg

const (
	AuthModeAccountKey       = "account-key"
	AuthModeSasToken         = "sas-token"
	AuthModeConnectionString = "connection-string"
)

type StorageAccount struct {
	Name     string `mapstructure:"name,omitempty" yaml:"name,omitempty" json:"name,omitempty"`
	Account  string `mapstructure:"account,omitempty" yaml:"account,omitempty" json:"account,omitempty"`
	AuthMode string `mapstructure:"auth-mode,omitempty"  yaml:"auth-mode,omitempty" json:"auth-mode,omitempty"`

	AccountKey       string `mapstructure:"account-key,omitempty" yaml:"account-key,omitempty" json:"account-key,omitempty"`
	SasToken         string `mapstructure:"sas-token,omitempty" yaml:"sas-token,omitempty" json:"sas-token,omitempty"`
	ConnectionString string `mapstructure:"conn-string,omitempty" yaml:"conn-string,omitempty" json:"conn-string,omitempty"`
}

type Option func(cfg *StorageAccount)

func WithName(k string) Option {
	return func(cfg *StorageAccount) {
		cfg.Name = k
	}
}

func WithAccountKey(k string) Option {
	return func(cfg *StorageAccount) {
		cfg.AccountKey = k
		cfg.AuthMode = AuthModeAccountKey
	}
}

func WithSasToken(t string) Option {
	return func(cfg *StorageAccount) {
		cfg.SasToken = t
		cfg.AuthMode = AuthModeSasToken
	}
}

func WithConnectionString(cs string) Option {
	return func(cfg *StorageAccount) {
		cfg.ConnectionString = cs
		cfg.AuthMode = AuthModeConnectionString
	}
}

/*
func ReadConfig(fileName string) (StorageAccountKeys, error) {

	stg := StorageAccountKeys{}

	wd, _ := os.Getwd()
	log.Info().Str("wd", wd).Msg("working dir")

	var b []byte

	configPath := util.FindFileInClosestDirectory(".", fileName)
	if configPath == "" {
		return stg, fmt.Errorf("cannot find config file of name %s", fileName)
	}

	log.Info().Str("file-name", configPath).Msg("found config file")

	b, err := ioutil.ReadFile(configPath)
	if err != nil {
		return stg, err
	}

	err = yaml.Unmarshal(b, &stg)
	if err != nil {
		return stg, err
	}

	if stg.AccountName == "" || stg.AccountKey == "" {
		return stg, fmt.Errorf("config file %s does not contain storage info", configPath)
	}

	return stg, nil
}

*/
