package azstoragecfg_test

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/storage/azstoragecfg"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	"testing"
)

func TestConfig(t *testing.T) {

	stgConfig := azstoragecfg.Config{
		Name:       "test",
		Account:    "${AZCOMMON_BLOB_ACCOUNTNAME}",
		AccountKey: "${AZCOMMON_BLOB_ACCOUNTKEY}",
		AuthMode:   azstoragecfg.AuthModeAccountKey,
	}

	b, err := yaml.Marshal(stgConfig)
	require.NoError(t, err)

	t.Log(string(b))
}
