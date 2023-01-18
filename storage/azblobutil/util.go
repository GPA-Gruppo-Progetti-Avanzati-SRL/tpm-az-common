package azblobutil

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"net/http"
)

type AzBlobError struct {
	StatusCode  int    `yaml:"status-code,omitempty" mapstructure:"status-code,omitempty" json:"status-code,omitempty"`
	ErrorCode   string `yaml:"error-code,omitempty" mapstructure:"error-code,omitempty" json:"error-code,omitempty"`
	Description string `yaml:"description-code,omitempty" mapstructure:"description-code,omitempty" json:"description-code,omitempty"`
}

func (e *AzBlobError) Error() string {
	return fmt.Sprintf("status-code:%d, error-code: %s, msg: \n%s", e.StatusCode, e.ErrorCode, e.Description)
}

func GetErrorStatusAndMessage(err error) (int, string) {
	if respErr, ok := err.(*azcore.ResponseError); ok {
		return respErr.StatusCode, respErr.ErrorCode
	}

	return 500, "InternalServerError"
}

func MapError2AzBlobError(err error) *AzBlobError {
	if respErr, ok := err.(*azcore.ResponseError); ok {
		return &AzBlobError{StatusCode: respErr.StatusCode, ErrorCode: respErr.ErrorCode, Description: respErr.Error()}
	}

	return &AzBlobError{StatusCode: http.StatusInternalServerError, ErrorCode: "InternalServerError", Description: err.Error()}
}
