package cosutil

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net/http"
)

func ConnectionStringFromEndpointAndAccountKey(ep, acctKey string) string {
	return fmt.Sprintf("AccountEndpoint=%s;AccountKey=%s;", ep, acctKey)
}

type CosError struct {
	Code int
	Text string
}

func (e *CosError) Error() string {
	return fmt.Sprintf("%s (%d)", e.Text, e.Code)
}

var EntityAlreadyExists = &CosError{Code: http.StatusConflict, Text: "entity already exists"}
var EntityNotFound = &CosError{Code: http.StatusNotFound, Text: "entity not found"}
var PreconditionFailed = &CosError{Code: http.StatusPreconditionFailed, Text: "precondition failed"}

// var InternalServerError = &CosError{Code: http.StatusInternalServerError, Text: "internal server error"}

func GetErrorStatusAndMessage(err error) (int, string) {
	const semLogContext = "cos-util::get-error-status-and-message"
	if respErr, ok := err.(*azcore.ResponseError); ok {
		if respErr.StatusCode != http.StatusNotFound {
			log.Error().Err(err).Msg(semLogContext)
		}
		return respErr.StatusCode, respErr.ErrorCode
	}
	return 500, "internal server error"
}

func MapAzCoreError(err error) error {
	const semLogContext = "cos-util::map-az-core-error"

	var zeroLogEvt *zerolog.Event
	st, msg := GetErrorStatusAndMessage(err)
	switch st {
	case http.StatusNotFound:
		zeroLogEvt = log.Info()
		err = EntityNotFound
	case http.StatusConflict:
		zeroLogEvt = log.Warn()
		err = EntityAlreadyExists
	case http.StatusPreconditionFailed:
		zeroLogEvt = log.Warn()
		err = PreconditionFailed
	default:
		err = &CosError{Code: st, Text: msg}
		zeroLogEvt = log.Error()
		zeroLogEvt = zeroLogEvt.Err(err)
	}

	zeroLogEvt.Int("http-status", st).Str("error-code", msg).Msg(semLogContext)
	return err
}

func IsNotFound(err error) bool {
	if respErr, ok := err.(*azcore.ResponseError); ok {
		log.Trace().Int("http-status", respErr.StatusCode).Str("error-code", respErr.ErrorCode).Send()
		return respErr.StatusCode == http.StatusNotFound
	}

	return false
}

func IsConflict(err error) bool {
	if respErr, ok := err.(*azcore.ResponseError); ok {
		log.Warn().Int("http-status", respErr.StatusCode).Str("error-code", respErr.ErrorCode).Send()
		return respErr.StatusCode == http.StatusConflict
	}

	return false
}

func IsPreconditionFailed(err error) bool {
	if respErr, ok := err.(*azcore.ResponseError); ok {
		log.Warn().Int("http-status", respErr.StatusCode).Str("error-code", respErr.ErrorCode).Send()
		return respErr.StatusCode == http.StatusPreconditionFailed
	}

	return false
}
