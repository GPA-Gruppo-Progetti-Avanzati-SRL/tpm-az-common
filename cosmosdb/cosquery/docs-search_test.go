package cosquery_test

import (
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-lib/metrics"
	"io"
	"os"
	"testing"
	"time"
	"tpm-az-common/cosmosdb/cosquery"
)

const (
	ConnectionStringEnvVar = "AZ_COMMON_CS"
	DbName                 = "rtp_bconn_db"
	ContainerName          = "tokens"
	QueryDelau             = 2
)

func TestCosQuery(t *testing.T) {

	cs := os.Getenv(ConnectionStringEnvVar)
	require.NotEmpty(t, cs, "CosmosDb cs not set.... use env var "+ConnectionStringEnvVar)

	closer, err := InitTracing(t)
	require.NoError(t, err)
	if closer != nil {
		defer closer.Close()
	}

	qc, err := cosquery.NewClientInstance(
		nil,
		cosquery.WithConnectionString(cs),
		cosquery.WithDbName(DbName),
		cosquery.WithCollectionName(ContainerName),
		cosquery.WithTrace(nil, ""),
		cosquery.WithQueryText("select * from c"))
	require.NoError(t, err)

	defer qc.Close()

	resp, err := qc.Execute()
	require.NoError(t, err)
	t.Logf("%v", resp)

	for qc.HasNext() {
		time.Sleep(QueryDelau * time.Second)
		resp, err = qc.Next()
		require.NoError(t, err)
		t.Logf("%v", resp)
	}

}

const (
	JAEGER_SERVICE_NAME = "JAEGER_SERVICE_NAME"
)

func InitTracing(t *testing.T) (io.Closer, error) {

	if os.Getenv(JAEGER_SERVICE_NAME) == "" {
		t.Log("skipping jaeger config no vars in env.... (" + JAEGER_SERVICE_NAME + ")")
		return nil, nil
	}

	var tracer opentracing.Tracer
	var closer io.Closer

	jcfg, err := jaegercfg.FromEnv()
	if err != nil {
		log.Warn().Err(err).Msg("Unable to configure JAEGER from environment")
		return nil, err
	}

	tracer, closer, err = jcfg.NewTracer(
		jaegercfg.Logger(&jlogger{}),
		jaegercfg.Metrics(metrics.NullFactory),
	)
	if nil != err {
		log.Error().Err(err).Msg("Error in NewTracer")
		return nil, err
	}

	opentracing.SetGlobalTracer(tracer)

	return closer, nil
}

type jlogger struct{}

func (l *jlogger) Error(msg string) {
	log.Error().Msg("(jaeger) " + msg)
}

func (l *jlogger) Infof(msg string, args ...interface{}) {
	log.Info().Msgf("(jaeger) "+msg, args...)
}
