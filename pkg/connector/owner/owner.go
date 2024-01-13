package owner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/teslamotors/vehicle-command/internal/log"

	"github.com/teslamotors/vehicle-command/pkg/connector"
	"github.com/teslamotors/vehicle-command/pkg/protocol"
)

func readWithContext(ctx context.Context, r io.Reader, p []byte) ([]byte, error) {
	bytesRead := 0
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		n, err := r.Read(p[bytesRead:])
		bytesRead += n
		if err == io.EOF {
			return p[:bytesRead], nil
		}
		if err != nil {
			return p[:bytesRead], err
		}
		if bytesRead == len(p) {
			return p[:bytesRead], nil
		}
	}
}

var ErrVehicleNotAwake = protocol.NewError("vehicle unavailable: vehicle is offline or asleep", false, false)

/*
The regular expression below extracts domains from HTTP bodies:

	{
	  "response": null,
	  "error": "user out of region, use base URL: https://fleet-api.prd.na.vn.cloud.tesla.com, see https://developer.tesla.com/docs/fleet-api#regional-requirements",
	  "error_description": ""
	}
*/
var baseDomainRE = regexp.MustCompile(`use base URL: https://([-a-z0-9.]*)`)

type HttpError struct {
	Code    int
	Message string
}

func (e *HttpError) Error() string {
	if e.Message == "" {
		return http.StatusText(e.Code)
	}
	return e.Message
}

func (e *HttpError) MayHaveSucceeded() bool {
	if e.Code >= 400 && e.Code < 500 {
		return false
	}
	return e.Code != http.StatusServiceUnavailable
}

func (e *HttpError) Temporary() bool {
	return e.Code == http.StatusServiceUnavailable ||
		e.Code == http.StatusGatewayTimeout ||
		e.Code == http.StatusRequestTimeout ||
		e.Code == http.StatusMisdirectedRequest ||
		e.Code == http.StatusTooManyRequests
}

func SendFleetAPICommand(ctx context.Context, client *http.Client, userAgent, authHeader string, url string, command interface{}) ([]byte, error) {
	var body []byte
	var ok bool
	if body, ok = command.([]byte); !ok {
		var err error
		body, err = json.Marshal(command)
		if err != nil {
			return nil, err
		}
	}
	log.Debug("Sending request to %s: %s", url, body)
	request, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, &protocol.CommandError{Err: err, PossibleSuccess: false, PossibleTemporary: true}
	}

	request.Header.Set("User-Agent", userAgent)
	request.Header.Set("Content-type", "application/json")
	request.Header.Set("Authorization", authHeader)
	request.Header.Set("Accept", "*/*")

	result, err := client.Do(request)
	if err != nil {
		return nil, &protocol.CommandError{Err: err, PossibleSuccess: false, PossibleTemporary: true}
	}
	defer result.Body.Close()

	body = make([]byte, connector.MaxResponseLength+1)
	body, err = readWithContext(ctx, result.Body, body)
	if err != nil {
		return nil, &protocol.CommandError{Err: err, PossibleSuccess: true, PossibleTemporary: false}
	}

	if len(body) == connector.MaxResponseLength+1 {
		return nil, protocol.NewError("response exceeds maximum length", true, true)
	}

	log.Debug("Server returned %d: %s: %s", result.StatusCode, http.StatusText(result.StatusCode), body)
	switch result.StatusCode {
	case http.StatusOK:
		return body, nil
	case http.StatusUnprocessableEntity: // HTTP: 422 on commands endpoint means protocol is not supported (fallback to regular commands)
		return nil, protocol.ErrProtocolNotSupported
	case http.StatusServiceUnavailable:
		return nil, ErrVehicleNotAwake
	case http.StatusRequestTimeout:
		if bytes.Contains(body, []byte("vehicle is offline")) {
			return nil, ErrVehicleNotAwake
		}
	}
	return nil, &HttpError{Code: result.StatusCode, Message: string(body)}
}

func ValidTeslaDomainSuffix(domain string) bool {
	return strings.HasSuffix(domain, ".tesla.com") || strings.HasSuffix(domain, ".tesla.cn") || strings.HasSuffix(domain, ".teslamotors.com")
}

// Sends a command to a Fleet API REST endpoint. Returns the response body and an error. The
// response body is not necessarily nil if the error is set.
func (c *Connection) SendFleetAPICommand(ctx context.Context, endpoint string, command interface{}) ([]byte, error) {
	url := fmt.Sprintf("https://%s/%s", c.serverURL, endpoint)
	rsp, err := SendFleetAPICommand(ctx, &c.client, c.UserAgent, c.authHeader, url, command)
	if err != nil {
		var httpErr *HttpError
		if errors.As(err, &httpErr) && httpErr.Code == http.StatusMisdirectedRequest {
			matches := baseDomainRE.FindStringSubmatch(httpErr.Message)
			if len(matches) == 2 && ValidTeslaDomainSuffix(matches[1]) {
				log.Debug("Received HTTP Status 421. Updating server URL.")
				c.serverURL = matches[1]
			}
		}
	}
	return rsp, err
}

func (c *Connection) Wakeup(ctx context.Context) error {
	type wakeResponse struct {
		State string `json:"state"`
	}
	var response struct {
		WakeResponse wakeResponse `json:"response"`
	}

	for {
		c.wakeLock.Lock()
		c.lastPoke = time.Now()
		c.wakeLock.Unlock()
		endpoint := fmt.Sprintf("api/1/vehicles/%s/wake_up", c.vin)
		respJSON, err := c.SendFleetAPICommand(ctx, endpoint, nil)
		if err == nil {
			err = json.Unmarshal(respJSON, &response)
			if err == nil && response.WakeResponse.State == "online" {
				return nil
			}
		}

		if !protocol.Temporary(err) {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
			continue
		}
	}
}
