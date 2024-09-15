package proxycore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestError(t *testing.T) {
	a := UnexpectedResponse{
		Expected: []string{"expected", "string"},
		Received: "received string",
	}

	res := a.Error()

	assert.NotEmptyf(t, res, "should not be empty")

}
