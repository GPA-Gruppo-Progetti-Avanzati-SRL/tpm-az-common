package coslease

import (
	"encoding/json"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/rs/zerolog/log"
	"regexp"
	"strings"
	"time"
)

const (
	LeaseTypeBlobEvent = "blob-event"
)

var LeasedObjectIdRegexpPattern = regexp.MustCompile("^([a-zA-Z0-9-_]*):([a-zA-Z0-9-_]*):([a-zA-Z0-9-_]*)$")
var leasedObjectNamePattern = "%s:%s:%s"
var LeaseIdRegexpPattern = regexp.MustCompile("^([a-zA-Z0-9-_]*):([a-zA-Z0-9-_]*):([a-zA-Z0-9-_]*):([a-zA-Z0-9-_]*)$")

var ZeroLease = Lease{}

type Lease struct {
	Id       string `mapstructure:"id,omitempty" yaml:"id,omitempty" json:"id,omitempty"`
	PKey     string `mapstructure:"pkey,omitempty" yaml:"pkey,omitempty" json:"pkey,omitempty"`
	LeaseId  string `mapstructure:"lease-id,omitempty" yaml:"lease-id,omitempty" json:"lease-id,omitempty"`
	Typ      string `mapstructure:"typ,omitempty" yaml:"typ,omitempty" json:"typ,omitempty"`
	Status   string `mapstructure:"status,omitempty" yaml:"status,omitempty" json:"status,omitempty"`
	Duration int    `mapstructure:"duration-secs,omitempty" yaml:"duration-secs,omitempty" json:"duration-secs,omitempty"`
	Ts       string `mapstructure:"ts,omitempty" yaml:"ts,omitempty" json:"ts,omitempty"`
	Ttl      int    `mapstructure:"ttl,omitempty" yaml:"ttl,omitempty" json:"ttl,omitempty"`
}

func NewLease(leaseType string, obkPkey, objId string, durationSecs int) Lease {

	leasedObjectId := LeasedObjectId(leaseType, obkPkey, objId)
	lid := strings.Join([]string{leasedObjectId, util.NewObjectId().String()}, ":")

	l := Lease{
		Id:       leasedObjectId,
		PKey:     leasedObjectId,
		LeaseId:  lid,
		Typ:      leaseType,
		Status:   "leased",
		Duration: durationSecs,
		Ts:       time.Now().Format(time.RFC3339Nano),
		Ttl:      300,
	}

	return l
}

func (l *Lease) PartitionKeyObjectId() (string, string, string, error) {
	matches := LeasedObjectIdRegexpPattern.FindAllSubmatch([]byte(l.Id), -1)
	for _, m := range matches {
		return string(m[1]), string(m[2]), string(m[3]), nil
	}

	return "", "", "", fmt.Errorf("cannot parse leased object id (%s)", l.Id)
}

func LeasedObjectId(leaseType string, obkPkey, objId string) string {
	return fmt.Sprintf(leasedObjectNamePattern, leaseType, obkPkey, objId)
}

func PartitionKeyObjectIdFromLeaseId(lid string) (string, string, string, error) {
	matches := LeaseIdRegexpPattern.FindAllSubmatch([]byte(lid), -1)
	for _, m := range matches {
		return string(m[1]), string(m[2]), string(m[3]), nil
	}

	return "", "", "", fmt.Errorf("cannot parse lease id (%s)", lid)
}

func (l *Lease) Expired() bool {
	if l.Ts != "" && l.Duration > 0 {
		ts, err := time.Parse(time.RFC3339Nano, l.Ts)
		if err != nil {
			log.Error().Err(err).Str("ts", l.Ts).Msg("cannot parse ts as RFC3339Nano")
			return true
		}

		var td time.Duration
		td = time.Duration(l.Duration) * time.Second
		if time.Now().Sub(ts) > td {
			return true
		}
		return false
	}

	return true
}

func (c *Lease) ToJSON() ([]byte, error) {
	return json.Marshal(c)
}

func (c *Lease) MustToJSON() []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}

	return b
}

func DeserializeEventDocument(b []byte) (*Lease, error) {
	ctx := Lease{}
	err := json.Unmarshal(b, &ctx)
	if err != nil {
		return nil, err
	}

	return &ctx, nil
}

func (l *Lease) Acquirable() bool {
	return !(l.Status == "leased" && !l.Expired())
}
