package vehicle

import (
	"context"

	carserver "github.com/teslamotors/vehicle-command/pkg/protocol/protobuf/carserver"
)

func (v *Vehicle) CreateStreamSession(ctx context.Context, sessionId string) error {
	return v.executeCarServerAction(ctx,
		&carserver.Action_VehicleAction{
			VehicleAction: &carserver.VehicleAction{
				VehicleActionMsg: &carserver.VehicleAction_CreateStreamSession{
					CreateStreamSession: &carserver.CreateStreamSession{
						SessionId: sessionId,
					},
				},
			},
		})
}

func (v *Vehicle) SendStreamMessage(ctx context.Context, sessionId string, data string) error {
	return v.executeCarServerAction(ctx,
		&carserver.Action_VehicleAction{
			VehicleAction: &carserver.VehicleAction{
				VehicleActionMsg: &carserver.VehicleAction_StreamMessage{
					StreamMessage: &carserver.StreamMessage{
						SessionId: sessionId,
						Data:      data,
					},
				},
			},
		})
}
