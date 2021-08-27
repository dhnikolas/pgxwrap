package pgxwrap

import (
	"context"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/opentracing/opentracing-go"
)

type Wrapper struct {
	primary *pgxpool.Pool
	replica *pgxpool.Pool
}

func New(primary, replica *pgxpool.Pool) *Wrapper {
	wrapper := &Wrapper{primary: primary, replica: replica}
	return wrapper
}

func (w *Wrapper) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, sql)
	defer span.Finish()
	if len(args) > 0 {
		span.SetTag("args", args)
	}

	return w.replica.Query(ctx, sql, args...)
}

func (w *Wrapper) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	span, ctx := opentracing.StartSpanFromContext(ctx, sql)
	defer span.Finish()
	if len(args) > 0 {
		span.SetTag("args", args)
	}

	return w.replica.QueryRow(ctx, sql, args...)
}

func (w *Wrapper) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, sql)
	defer span.Finish()
	if len(args) > 0 {
		span.SetTag("args", args)
	}

	return w.primary.Exec(ctx, sql, args...)
}

func (w *Wrapper) Primary() *pgxpool.Pool {
	return w.primary
}

func (w *Wrapper) Replica() *pgxpool.Pool {
	return w.replica
}
