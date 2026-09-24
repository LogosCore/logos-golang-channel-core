package amqprpc

import (
	"context"
	"encoding/json"
	"fmt"

	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
	"github.com/logoscore/logos-golang-channel-core/pkg/mgmtrpc"
	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
	amqp "github.com/rabbitmq/amqp091-go"
)

// amqpChannel abstracts the AMQP channel operations for testability.
type amqpChannel interface {
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	ConsumeWithContext(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

type request struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type response struct {
	OK    bool          `json:"ok"`
	Data  any           `json:"data,omitempty"`
	Error *errorPayload `json:"error,omitempty"`
}

type errorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type profileIDParam struct {
	ProfileID int32 `json:"profile_id"`
}

// QueueName returns the RPC queue name for a given channel instance.
func QueueName(channelID string) string {
	return fmt.Sprintf("channel.profiles.%s", channelID)
}

// StartRPCConsumer starts consuming RPC messages from the per-channel profile management queue.
// It blocks until ctx is cancelled or an unrecoverable error occurs.
func StartRPCConsumer(ctx context.Context, conn *amqp.Connection, channelID string, server *mgmtrpc.Server) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open amqp channel: %w", err)
	}
	defer ch.Close()
	return consumeLoop(ctx, ch, channelID, server)
}

// consumeLoop runs the consume loop on an amqpChannel interface (for testability).
func consumeLoop(ctx context.Context, ch amqpChannel, channelID string, server *mgmtrpc.Server) error {
	queue := QueueName(channelID)
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %q: %w", queue, err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("set qos: %w", err)
	}

	deliveries, err := ch.ConsumeWithContext(ctx, queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume queue %q: %w", queue, err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			resp := handleDelivery(ctx, channelID, server, d.Body)
			if d.ReplyTo != "" {
				body, _ := json.Marshal(resp)
				_ = ch.PublishWithContext(ctx, "", d.ReplyTo, false, false, amqp.Publishing{
					ContentType:   "application/json",
					CorrelationId: d.CorrelationId,
					Body:          body,
				})
			}
			_ = d.Ack(false)
		}
	}
}

func handleDelivery(ctx context.Context, channelID string, server *mgmtrpc.Server, body []byte) response {
	var req request
	if err := json.Unmarshal(body, &req); err != nil {
		return errResponse(coreerrors.CodeInvalidInput, "malformed JSON request")
	}

	switch req.Method {
	case "profile.create":
		return handleProfileMutation(ctx, channelID, server, req.Params, func(ctx context.Context, p profile.Profile) error {
			return server.CreateProfile(ctx, channelID, p)
		})
	case "profile.update":
		return handleProfileMutation(ctx, channelID, server, req.Params, func(ctx context.Context, p profile.Profile) error {
			return server.UpdateProfile(ctx, channelID, p)
		})
	case "profile.read":
		return handleProfileID(ctx, req.Params, func(id int32) (any, error) {
			return server.ReadProfile(ctx, channelID, id)
		})
	case "profile.delete":
		return handleProfileID(ctx, req.Params, func(id int32) (any, error) {
			return nil, server.DeleteProfile(ctx, channelID, id)
		})
	case "profile.activate":
		return handleProfileID(ctx, req.Params, func(id int32) (any, error) {
			return nil, server.ActivateProfile(ctx, channelID, id)
		})
	case "profile.deactivate":
		return handleProfileID(ctx, req.Params, func(id int32) (any, error) {
			return nil, server.DeactivateProfile(ctx, channelID, id)
		})
	case "profile.list":
		profiles, err := server.ListProfiles(ctx, channelID)
		if err != nil {
			return errFromError(err)
		}
		return okResponse(profiles)
	default:
		return errResponse(coreerrors.CodeNotImplemented, "unknown method: "+req.Method)
	}
}

func handleProfileMutation(_ context.Context, _ string, _ *mgmtrpc.Server, params json.RawMessage, fn func(context.Context, profile.Profile) error) response {
	var p profile.Profile
	if err := json.Unmarshal(params, &p); err != nil {
		return errResponse(coreerrors.CodeInvalidInput, "invalid profile params: "+err.Error())
	}
	if err := fn(context.Background(), p); err != nil {
		return errFromError(err)
	}
	return okResponse(nil)
}

func handleProfileID(_ context.Context, params json.RawMessage, fn func(int32) (any, error)) response {
	var pid profileIDParam
	if err := json.Unmarshal(params, &pid); err != nil {
		return errResponse(coreerrors.CodeInvalidInput, "invalid params: "+err.Error())
	}
	if pid.ProfileID <= 0 {
		return errResponse(coreerrors.CodeInvalidInput, "profile_id must be a positive integer")
	}
	data, err := fn(pid.ProfileID)
	if err != nil {
		return errFromError(err)
	}
	return okResponse(data)
}

func okResponse(data any) response {
	return response{OK: true, Data: data}
}

func errResponse(code, message string) response {
	return response{OK: false, Error: &errorPayload{Code: code, Message: message}}
}

func errFromError(err error) response {
	return errResponse(coreerrors.Code(err), err.Error())
}
