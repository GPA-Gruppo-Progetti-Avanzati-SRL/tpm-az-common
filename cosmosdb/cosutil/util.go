package cosutil

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net/http"
)

func ConnectionStringFromEndpointAndAccountKey(ep, acctKey string) string {
	return fmt.Sprintf("AccountEndpoint=%s;AccountKey=%s;", ep, acctKey)
}

var EntityAlreadyExists = errors.New("entity already exists")
var EntityNotFound = errors.New("entity not found")
var PreconditionFailed = errors.New("precondition failed")
var InternalServerError = errors.New("internal server error")

func GetErrorStatusAndMessage(err error) (int, string) {
	if respErr, ok := err.(*azcore.ResponseError); ok {
		return respErr.StatusCode, respErr.ErrorCode
	}
	return 500, "InternalServerError"
}

func MapAzCoreError(err error) error {

	const semLogContext = "az error: mapping az-core error"

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
		err = InternalServerError
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
