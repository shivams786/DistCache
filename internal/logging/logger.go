package logging

import (
	"log/slog"
	"os"
)

func New(nodeID string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(handler).With("node_id", nodeID)
}
