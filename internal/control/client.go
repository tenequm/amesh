package control

import (
	"context"
	"errors"
	"io"
)

func Pipe(ctx context.Context, socket string, args []string, in io.Reader, out io.Writer) error {
	return errors.ErrUnsupported
}

func Hook(ctx context.Context, socket string, in io.Reader, out io.Writer) error {
	return errors.ErrUnsupported
}

func Status(ctx context.Context, socket string, out io.Writer) error { return errors.ErrUnsupported }
