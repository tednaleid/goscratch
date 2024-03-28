package main

import (
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
)

func TestParseResponse(t *testing.T) {
	mockResponse := &http.Response{
		Body: ioutil.NopCloser(strings.NewReader(`{"main": {"temp": 20.5}}`)),
	}

	data := parseResponse(mockResponse)

	if data.Main.Temp != 20.5 {
		t.Errorf("Expected temperature to be 20.5, but got %v", data.Main.Temp)
	}
}
