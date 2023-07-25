package azblobevent

import (
	"time"
)

type Config struct {
	CosName           string        `mapstructure:"cos-name,omitempty" yaml:"cos-name,omitempty" json:"cos-name,omitempty"`
	ProcessedEventTtl int           `mapstructure:"processed-event-ttl,omitempty" yaml:"processed-event-ttl,omitempty" json:"processed-event-ttl,omitempty"`
	TickInterval      time.Duration `mapstructure:"tick-interval" yaml:"tick-interval" json:"tick-interval"`
	ExitOnNop         bool          `mapstructure:"exit-on-nop" yaml:"exit-on-nop" json:"exit-on-nop"`
	ExitOnErr         bool          `mapstructure:"exit-on-err" yaml:"exit-on-err" json:"exit-on-err"`
}

func (c *Config) PostProcess() error {
	const semLogContext = "azb-event-crawler::cfg-post-process"
	return nil
}
