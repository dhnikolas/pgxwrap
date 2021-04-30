package pgxwrap

import (
	"context"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/opentracing/opentracing-go"
)

type Wrapper struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Wrapper {
	wrapper := &Wrapper{db: db}
	return wrapper
}

func (w *Wrapper) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, sql)
	defer span.Finish()
	if len(args) > 0 {
		span.SetTag("args", args)
	}

	return w.db.Query(ctx, sql, args)
}

func (w *Wrapper) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	span, ctx := opentracing.StartSpanFromContext(ctx, sql)
	defer span.Finish()
	if len(args) > 0 {
		span.SetTag("args", args)
	}

	return w.db.QueryRow(ctx, sql, args)
}

func (w *Wrapper) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, sql)
	defer span.Finish()
	if len(args) > 0 {
		span.SetTag("args", args)
	}

	return w.db.Exec(ctx, sql, args)
}

func (w *Wrapper) Db() *pgxpool.Pool {
	return w.db
}
