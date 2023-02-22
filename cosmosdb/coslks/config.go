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
	CosmosName  string         `mapstructure:"cos-name,omitempty" yaml:"cos-name,omitempty" json:"cos-name,omitempty"`
	Endpoint    string         `mapstructure:"endpoint,omitempty" yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	AccountKey  string         `yaml:"account-key,omitempty" mapstructure:"account-key,omitempty" json:"account-key,omitempty"`
	DB          KeyNamePair    `yaml:"db,omitempty" mapstructure:"db,omitempty" json:"db,omitempty"`
	Collections CollectionsCfg `yaml:"collections,omitempty" mapstructure:"collections,omitempty" json:"collections,omitempty"`
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

	const semLogContext = "cos-lks::get-db-name-by-id"
	if c.DB.Id == id {
		return c.DB.Name
	}

	log.Error().Str("db-id", id).Msg(semLogContext + " can't find db by id")
	return ""
}
