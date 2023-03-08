package azblobcrawler

import (
	"errors"
	"github.com/rs/zerolog/log"
	"regexp"
	"time"
)

type Mode string

const (
	ModeTag Mode = "tag"
)

type TagValueId string

const (
	TagValueReady   TagValueId = "ready"
	TagValueWorking TagValueId = "working"
	TagValueDone    TagValueId = "done"
)

type TagValue struct {
	Id    TagValueId `mapstructure:"id,omitempty" yaml:"id,omitempty" json:"id,omitempty"`
	Value string     `mapstructure:"value,omitempty" yaml:"value,omitempty" json:"value,omitempty"`
}

type Path struct {
	Container   string         `mapstructure:"container,omitempty" yaml:"container,omitempty" json:"container,omitempty"`
	NamePattern string         `mapstructure:"pattern,omitempty" yaml:"pattern,omitempty" json:"pattern,omitempty"`
	Id          string         `mapstructure:"id,omitempty" yaml:"id,omitempty" json:"id,omitempty"`
	Regexp      *regexp.Regexp `mapstructure:"-" yaml:"-" json:"-"`
}

type Config struct {
	StorageName  string        `mapstructure:"storage-name,omitempty" yaml:"storage-name,omitempty" json:"storage-name,omitempty"`
	Mode         Mode          `mapstructure:"mode,omitempty" yaml:"mode,omitempty" json:"mode,omitempty"`
	TagName      string        `mapstructure:"tag-name,omitempty" yaml:"tag-name,omitempty" json:"tag-name,omitempty"`
	Tags         []TagValue    `mapstructure:"tag-values,omitempty" yaml:"tag-values,omitempty" json:"tag-values,omitempty"`
	Paths        []Path        `mapstructure:"paths,omitempty" yaml:"paths,omitempty" json:"paths,omitempty"`
	TickInterval time.Duration `mapstructure:"tick-interval" yaml:"tick-interval" json:"tick-interval"`
	DownloadPath string        `mapstructure:"download-path" yaml:"download-path" json:"download-path"`
	ExitOnNop    bool          `mapstructure:"exit-on-nop" yaml:"exit-on-nop" json:"exit-on-nop"`
	ExitOnErr    bool          `mapstructure:"exit-on-err" yaml:"exit-on-err" json:"exit-on-err"`
}

func (c *Config) GetTagByType(tt TagValueId) (TagValue, bool) {
	for _, t := range c.Tags {
		if t.Id == tt {
			return t, true
		}
	}

	return TagValue{}, false
}

func (c *Config) PostProcess() error {

	const semLogContext = "azb-crawler::cfg-post-process"

	var err error

	if len(c.Paths) == 0 {
		err = errors.New("no paths configured in blob crawler")
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	for i, p := range c.Paths {
		c.Paths[i].Regexp, err = regexp.Compile(p.NamePattern)
		if err != nil {
			log.Error().Str("pattern", p.NamePattern).Err(err).Msg(semLogContext)
			return err
		}
	}

	return nil
}
