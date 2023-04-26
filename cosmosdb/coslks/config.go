package coslks

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type KeyNamePair struct {
	Id   string
	Name string
}

type CollectionsCfg []KeyNamePair

type Config struct {
	CosmosName  string         `mapstructure:"cos-name,omitempty" yaml:"cos-name,omitempty" json:"cos-name,omitempty"`
	Endpoint    string         `mapstructure:"endpoint,omitempty" yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	AccountKey  string         `yaml:"account-key,omitempty" mapstructure:"account-key,omitempty" json:"account-key,omitempty"`
	DB          KeyNamePair    `yaml:"db,omitempty" mapstructure:"db,omitempty" json:"db,omitempty"`
	Collections CollectionsCfg `yaml:"collections,omitempty" mapstructure:"collections,omitempty" json:"collections,omitempty"`
}

func (c *Config) PostProcess() error {
	return nil
}

func (c *Config) GetCollectionNameById(aCollectionId string) string {

	const semLogContext = "cos-lks::get-collection-name-by-id"
	for _, c := range c.Collections {
		if c.Id == aCollectionId {
			return c.Name
		}
	}

	var evt *zerolog.Event
	if len(c.Collections) > 0 {
		evt = log.Warn()
	} else {
		evt = log.Info()
	}

	evt.Str("coll-id", aCollectionId).Msg(semLogContext + " not found")

	return ""
}

func (c *Config) GetDbNameById(id string) string {

	const semLogContext = "cos-lks::get-db-name-by-id"
	if c.DB.Id == id {
		return c.DB.Name
	}

	var evt *zerolog.Event
	if c.DB.Id != "" {
		evt = log.Warn()
	} else {
		evt = log.Info()
	}

	evt.Str("db-id", id).Msg(semLogContext + " not found")
	return ""
}
