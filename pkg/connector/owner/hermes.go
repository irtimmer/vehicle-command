package owner

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/teslamotors/vehicle-command/internal/log"
	"github.com/teslamotors/vehicle-command/pkg/connector"
	"github.com/teslamotors/vehicle-command/pkg/protocol/protobuf/hermes"
	"github.com/teslamotors/vehicle-command/pkg/protocol/protobuf/universalmessage"
	"google.golang.org/protobuf/proto"
)

var serverURL = "wss://signaling.vn.teslamotors.com:443/v1/mobile"
var maxLatency = 4 * time.Second

type Connection struct {
	UserAgent  string
	vin        string
	inbox      chan []byte
	serverURL  string
	authHeader string
	websocket  *websocket.Conn
	client     http.Client

	lock         sync.Mutex
	userToken    string
	vehicleToken string
	errChan      chan error
	wakeLock     sync.Mutex
	lastPoke     time.Time

	streamingConfig string
}

// AllowedLatency implements connector.Connector.
func (c *Connection) AllowedLatency() time.Duration {
	return maxLatency
}

func webSocketConnect(serverURL, userToken string) (*websocket.Conn, error) {
	log.Debug("Opening Websocket")

	headers := http.Header{
		"X-Jwt": {userToken},
	}
	client, _, err := websocket.DefaultDialer.Dial(serverURL, headers)

	if err != nil {
		return nil, err
	}

	return client, err
}

func NewConnection(vin, userToken, vehicleToken string) (*Connection, error) {
	var err error

	websocket, err := webSocketConnect(serverURL, userToken)
	if err != nil {
		return nil, err
	}

	conn := Connection{
		vin:          vin,
		websocket:    websocket,
		inbox:        make(chan []byte, 5),
		userToken:    userToken,
		vehicleToken: vehicleToken,
		errChan:      make(chan error, 1),
	}

	go func() {
		for {
			_, message, err := websocket.ReadMessage()
			if err != nil {
				conn.errChan <- err
			}

			conn.newMessage(message)
		}
	}()

	select {
	case err := <-conn.errChan:
		return &conn, err
	default:
	}

	return &conn, err
}

func (c *Connection) newMessage(buffer []byte) error {
	var err error
	message := &hermes.HermesMessage{}
	if err := proto.Unmarshal(buffer, message); err != nil {
		return err
	}
	log.Debug("Received from server " + string(message.GetCommandMessage().String()))
	payload := message.CommandMessage.GetPayload()

	if !statusCodeOK(message.GetCommandMessage().GetStatusCode()) {
		return fmt.Errorf("Received Status NOT OK: " + string(message.GetCommandMessage().GetPayload()))
	}

	// Put only command response payloads into the inbox
	commandType := message.GetCommandMessage().GetCommandType()
	if commandType == *hermes.CommandType_COMMAND_TYPE_SIGNED_COMMAND_RESPONSE.Enum() {

		decoded := &universalmessage.RoutableMessage{}
		if err := proto.Unmarshal(payload, decoded); err != nil {
			return err
		}
		log.Debug("Decoded Payload: " + decoded.String())

		select {
		case c.inbox <- payload:
		default:
			return fmt.Errorf("dropped response due to full inbox")
		}
	} else if commandType == *hermes.CommandType_COMMAND_TYPE_STREAMING_CONFIG.Enum() {
		// Store the streaming config
		c.streamingConfig = string(payload)
	}
	// ACK every message
	err = c.sendAck(message)
	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) sendAck(input *hermes.HermesMessage) error {
	requestTxid := input.GetCommandMessage().GetTxid()
	topic := input.GetCommandMessage().GetTopic()
	uuid := uuid.New()
	output := &hermes.HermesMessage{
		CommandMessage: &hermes.CommandMessage{
			Txid:        []byte(uuid.String()),
			Topic:       topic,
			RequestTxid: requestTxid,
			StatusCode:  *hermes.StatusCode_STATUS_CODE_CLIENT_ACK.Enum(),
		},
	}

	err := c.sendMessage(output)
	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) sendMessage(output *hermes.HermesMessage) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	log.Debug("Sending Message: " + output.String())

	encoded, err := proto.Marshal(output)
	if err != nil {
		return err
	}

	err = c.websocket.WriteMessage(websocket.BinaryMessage, encoded)
	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) PreferredAuthMethod() connector.AuthMethod {
	return connector.AuthMethodHMAC
}

func (c *Connection) RetryInterval() time.Duration {
	return time.Second
}

func (c *Connection) Receive() <-chan []byte {
	return c.inbox
}

func (c *Connection) Close() {
	c.websocket.Close()
	if c.inbox != nil {
		close(c.inbox)
		c.inbox = nil
	}
}

func statusCodeOK(code hermes.StatusCode) bool {
	switch code {
	case
		*hermes.StatusCode_STATUS_CODE_OK.Enum(),
		*hermes.StatusCode_STATUS_CODE_CLIENT_ACK.Enum(),
		*hermes.StatusCode_STATUS_CODE_SERVER_ACK.Enum(),
		*hermes.StatusCode_STATUS_CODE_APPLICATION_OK.Enum(),
		*hermes.StatusCode_STATUS_CODE_APPLICATION_ACK.Enum():
		return true
	}
	return false
}

func (c *Connection) VIN() string {
	return c.vin
}

func (c *Connection) StreamingConfig() string {
	return c.streamingConfig
}

func (c *Connection) Send(ctx context.Context, buffer []byte) error {

	topic := "vehicle_device." + c.vin + ".cmds"
	uuid := uuid.New()
	output := &hermes.HermesMessage{
		CommandMessage: &hermes.CommandMessage{
			Txid:    []byte(uuid.String()),
			Topic:   []byte(topic),
			Expiry:  &hermes.Timestamp{Seconds: 10},
			Payload: buffer,
			Options: &hermes.FlatbuffersMessageOptions{
				Token: []byte(c.vehicleToken),
			},
		},
	}

	err := c.sendMessage(output)
	if err != nil {
		return err
	}
	return nil
}
