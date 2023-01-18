package coslks

import (
	"github.com/rs/zerolog/log"
)

type KeyNamePair struct {
	Id   string
	Name string
}

type CollectionsCfg []KeyNamePair

type Config struct {
	Endpoint    string         `mapstructure:"endpoint" yaml:"endpoint" json:"endpoint"`
	AccountKey  string         `yaml:"account-key" mapstructure:"account-key" json:"account-key"`
	DB          KeyNamePair    `yaml:"db" mapstructure:"db" json:"db"`
	Collections CollectionsCfg `yaml:"collections" mapstructure:"collections" json:"collections"`
}

func (c *Config) PostProcess() error {
	return nil
}

func (c *Config) GetCollectionName(aCollectionId string) string {

	for _, c := range c.Collections {
		if c.Id == aCollectionId {
			return c.Name
		}
	}

	log.Error().Str("coll-id", aCollectionId).Msg("can't find collection by id")
	return aCollectionId
}

func (c *Config) GetDbName(id string) string {

	if c.DB.Id == id {
		return c.DB.Name
	}

	log.Error().Str("db-id", id).Msg("can't find db by id")
	return ""
}
