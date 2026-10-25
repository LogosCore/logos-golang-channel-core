package logstream

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// Handler wraps next so every record also reaches the Hub. Records below
// next's level are still built while a stream asks for debug, and go to the
// Hub only; next's output is unchanged.
func (h *Hub) Handler(next slog.Handler) slog.Handler {
	return &handler{hub: h, next: next}
}

type handler struct {
	hub    *Hub
	next   slog.Handler
	attrs  []slog.Attr // from WithAttrs, keys already group-qualified
	prefix string      // open groups, "a.b."
}

func (h *handler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.next.Enabled(ctx, l) || l >= slog.LevelInfo || h.hub.WantsDebug()
}

func (h *handler) Handle(ctx context.Context, r slog.Record) error {
	var err error
	if h.next.Enabled(ctx, r.Level) {
		err = h.next.Handle(ctx, r)
	}
	rec := protocol.LogRecord{Time: r.Time, Level: level(r.Level), Message: r.Message}
	if rec.Time.IsZero() {
		rec.Time = time.Now()
	}
	attrs := map[string]any{}
	add := func(key string, v slog.Value) bool {
		if len(attrs) >= protocol.MaxLogAttrs {
			return false
		}
		addAttr(attrs, key, v)
		return true
	}
	for _, a := range h.attrs {
		if !add(a.Key, a.Value) {
			break
		}
	}
	r.Attrs(func(a slog.Attr) bool { return add(h.prefix+a.Key, a.Value) })
	if len(attrs) > 0 {
		rec.Attrs = attrs
	}
	h.hub.Record(rec)
	return err
}

func (h *handler) WithAttrs(as []slog.Attr) slog.Handler {
	c := *h
	c.next = h.next.WithAttrs(as)
	c.attrs = append([]slog.Attr(nil), h.attrs...)
	for _, a := range as {
		c.attrs = append(c.attrs, slog.Attr{Key: h.prefix + a.Key, Value: a.Value})
	}
	return &c
}

func (h *handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	c := *h
	c.next = h.next.WithGroup(name)
	c.prefix = h.prefix + name + "."
	return &c
}

// addAttr flattens groups into dotted keys and renders values as JSON-safe
// scalars.
func addAttr(out map[string]any, key string, v slog.Value) {
	v = v.Resolve()
	switch v.Kind() {
	case slog.KindGroup:
		for _, a := range v.Group() {
			k := a.Key
			if key != "" {
				k = key + "." + k
			}
			addAttr(out, k, a.Value)
		}
		return
	case slog.KindString:
		out[key] = v.String()
	case slog.KindInt64:
		out[key] = v.Int64()
	case slog.KindUint64:
		out[key] = v.Uint64()
	case slog.KindFloat64:
		out[key] = v.Float64()
	case slog.KindBool:
		out[key] = v.Bool()
	case slog.KindDuration:
		out[key] = v.Duration().String()
	case slog.KindTime:
		out[key] = v.Time().Format(time.RFC3339Nano)
	default:
		if err, ok := v.Any().(error); ok {
			out[key] = err.Error()
		} else {
			out[key] = fmt.Sprint(v.Any())
		}
	}
}

func level(l slog.Level) protocol.LogLevel {
	switch {
	case l < slog.LevelInfo:
		return protocol.LogDebug
	case l < slog.LevelWarn:
		return protocol.LogInfo
	case l < slog.LevelError:
		return protocol.LogWarn
	default:
		return protocol.LogError
	}
}
