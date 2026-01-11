package entity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"

	"github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
)

// Package entity provides convenient wrapper functions for goqu query builder.

// ExecInsert executes an insert statement.
func ExecInsert(ctx context.Context, db DB, stmt *goqu.InsertDataset) (sql.Result, error) {
	if !stmt.IsPrepared() {
		stmt = stmt.Prepared(true)
	}

	query, args, err := stmt.ToSQL()
	if err != nil {
		return nil, fmt.Errorf("build insert statement, %w", err)
	}
	return db.ExecContext(ctx, query, args...)
}

// ExecUpdate executes an update statement.
func ExecUpdate(ctx context.Context, db DB, stmt *goqu.UpdateDataset) (sql.Result, error) {
	if !stmt.IsPrepared() {
		stmt = stmt.Prepared(true)
	}

	query, args, err := stmt.ToSQL()
	if err != nil {
		return nil, fmt.Errorf("build update statement, %w", err)
	}
	return db.ExecContext(ctx, query, args...)
}

// ExecDelete executes a delete statement.
func ExecDelete(ctx context.Context, db DB, stmt *goqu.DeleteDataset) (sql.Result, error) {
	if !stmt.IsPrepared() {
		stmt = stmt.Prepared(true)
	}

	query, args, err := stmt.Prepared(true).ToSQL()
	if err != nil {
		return nil, fmt.Errorf("build delete statement, %w", err)
	}
	return db.ExecContext(ctx, query, args...)
}

// GetRecord executes a select query and returns a single result.
func GetRecord(ctx context.Context, dest any, db DB, stmt *goqu.SelectDataset) error {
	if !stmt.IsPrepared() {
		stmt = stmt.Prepared(true)
	}

	query, args, err := stmt.ToSQL()
	if err != nil {
		return fmt.Errorf("build select statement, %w", err)
	}
	return db.GetContext(ctx, dest, query, args...)
}

// GetRecords executes a select query and returns multiple results.
func GetRecords(ctx context.Context, dest any, db DB, stmt *goqu.SelectDataset) error {
	if !stmt.IsPrepared() {
		stmt = stmt.Prepared(true)
	}

	query, args, err := stmt.ToSQL()
	if err != nil {
		return fmt.Errorf("build select statement, %w", err)
	}
	return db.SelectContext(ctx, dest, query, args...)
}

// GetTotalCount returns the total number of records matching the query conditions.
func GetTotalCount(ctx context.Context, db DB, stmt *goqu.SelectDataset) (int, error) {
	stmt = stmt.Select(goqu.L(`count(1)`))
	clauses := stmt.GetClauses()
	if clauses.HasOrder() {
		stmt = stmt.ClearOrder()
	}
	if clauses.HasLimit() {
		stmt = stmt.ClearLimit().ClearOffset()
	}

	var total int
	if err := GetRecord(ctx, &total, db, stmt); err != nil {
		return 0, err
	}
	return total, nil
}

// Transaction executes a function within a database transaction, committing on success or rolling back on error.
//
// Deprecated: Use TransactionX instead.
func Transaction[T Tx, U TxInitiator[T]](db U, fn func(db DB) error) (err error) {
	return TransactionX(context.Background(), db, fn)
}

// TransactionX executes a function within a database transaction with context support.
func TransactionX[T Tx, U TxInitiator[T]](ctx context.Context, db U, fn func(db DB) error) (err error) {
	return runTransaction(ctx, db, nil, fn)
}

// TransactionWithOptions executes a function within a database transaction with custom options.
//
// Deprecated: Use TransactionWithOptionsX instead.
func TransactionWithOptions[T Tx, U TxInitiator[T]](db U, opt *sql.TxOptions, fn func(db DB) error) (err error) {
	return TransactionWithOptionsX(context.Background(), db, opt, fn)
}

// TransactionWithOptionsX executes a function within a database transaction with context and custom options.
func TransactionWithOptionsX[T Tx, U TxInitiator[T]](ctx context.Context, db U, opt *sql.TxOptions, fn func(db DB) error) (err error) {
	return runTransaction(ctx, db, opt, fn)
}

// TryTransaction attempts to execute a function within a transaction. If the database is already a transaction, the function is executed directly. If it's a transaction initiator, a transaction is started.
//
// Deprecated: Use TryTransactionX instead.
func TryTransaction[T Tx](db DB, fn func(db DB) error) error {
	return TryTransactionX[T](context.Background(), db, fn)
}

// TryTransactionX attempts to execute a function within a transaction with context support.
// If the database is already a transaction, the function is executed directly.
// If it's a transaction initiator, a transaction is started.
// The specific Tx type must be explicitly specified as it cannot be derived from the DB interface.
//
// Example: TryTransactionX[*sqlx.Tx](ctx, db, func(db entity.DB) error { ... })
func TryTransactionX[T Tx](ctx context.Context, db DB, fn func(db DB) error) error {
	if v, ok := db.(T); ok {
		return fn(v)
	} else if v, ok := db.(TxInitiator[T]); ok {
		return TransactionX(ctx, v, fn)
	}

	var x T
	return fmt.Errorf("db is neither %T nor TxInitiator[%T]", x, x)
}

// TryTransactionWithOptions attempts to execute a function within a transaction with custom options.
//
// Deprecated: Use TryTransactionWithOptionsX instead.
func TryTransactionWithOptions[T Tx](db DB, opt *sql.TxOptions, fn func(db DB) error) error {
	return TryTransactionWithOptionsX[T](context.Background(), db, opt, fn)
}

// TryTransactionWithOptionsX attempts to execute a function within a transaction with context and custom options.
// If the database is already a transaction, the function is executed directly.
// If it's a transaction initiator, a transaction is started.
// The specific Tx type must be explicitly specified as it cannot be derived from the DB interface.
//
// Example: TryTransactionWithOptionsX[*sqlx.Tx](ctx, db, opt, func(db entity.DB) error { ... })
func TryTransactionWithOptionsX[T Tx](ctx context.Context, db DB, opt *sql.TxOptions, fn func(db DB) error) error {
	if v, ok := db.(T); ok {
		return fn(v)
	} else if v, ok := db.(TxInitiator[T]); ok {
		return TransactionWithOptionsX(ctx, v, opt, fn)
	}

	var x T
	return fmt.Errorf("db is neither %T nor TxInitiator[%T]", x, x)
}

func runTransaction[T Tx, U TxInitiator[T]](ctx context.Context, db U, opt *sql.TxOptions, fn func(db DB) error) (err error) {
	tx, err := db.BeginTxx(ctx, opt)
	if err != nil {
		return fmt.Errorf("begin transaction, %w", err)
	}

	defer func() {
		if v := recover(); v != nil {
			if vv, ok := v.(error); ok {
				err = vv
			} else {
				err = fmt.Errorf("%v", v)
			}

			if errRollback := tx.Rollback(); errRollback != nil {
				err = fmt.Errorf("rollback transaction, %v, caused by %w", errRollback, err)
			}
			return
		}

		if err == nil {
			if errCommit := tx.Commit(); errCommit != nil {
				err = fmt.Errorf("commit transaction, %w", errCommit)
			}
		} else {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = fmt.Errorf("rollback transaction, %v, caused by %w", errRollback, err)
			}
		}
	}()

	return fn(tx)
}

// TrySavePoint attempts to create a savepoint in a transaction. Returns an error if the database is not in a transaction.
func TrySavePoint(ctx context.Context, db DB, name string, fn func() error) error {
	if v, ok := db.(Tx); ok {
		return SavePoint(ctx, v, name, fn)
	}

	return errors.New("is not in transaction")
}

var validSavePointName = regexp.MustCompile(`^[0-9a-zA-Z_]+$`)

// SavePoint creates a savepoint in a transaction and releases it after the function executes successfully. If the function fails or panics, it rolls back to the savepoint.
func SavePoint(ctx context.Context, tx Tx, name string, fn func() error) (err error) {
	if !validSavePointName.MatchString(name) {
		return fmt.Errorf("invalid savepoint name: %s", name)
	}

	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SAVEPOINT %s", name)); err != nil {
		return fmt.Errorf("create savepoint %s, %w", name, err)
	}

	defer func() {
		if v := recover(); v != nil {
			if vv, ok := v.(error); ok {
				err = vv
			} else {
				err = fmt.Errorf("%v", v)
			}
		}

		if err == nil {
			if _, errRelease := tx.ExecContext(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s", name)); errRelease != nil {
				err = fmt.Errorf("release savepoint %s, %w", name, errRelease)
			}
		} else {
			if _, errRollback := tx.ExecContext(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name)); errRollback != nil {
				err = errors.Join(err,
					fmt.Errorf("rollback to savepoint %s, %w", name, errRollback),
				)
			}
		}
	}()

	return fn()
}

// QueryBy executes a select query and processes the result set using the provided callback function.
func QueryBy(ctx context.Context, db DB, stmt *goqu.SelectDataset, fn func(ctx context.Context, rows *sqlx.Rows) error) error {
	query, args, err := stmt.ToSQL()
	if err != nil {
		return fmt.Errorf("build sql, %w", err)
	}

	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute query, %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := fn(ctx, rows); err != nil {
			return fmt.Errorf("handle row, %w", err)
		}
	}
	return rows.Err()
}

// NewUpsertRecord builds a record for upsert operations.
// Fields marked as refuse update will not be updated. Use the columns parameter to update additional fields.
func NewUpsertRecord(ent Entity, otherColumns ...string) goqu.Record {
	md, err := getMetadata(ent)
	if err != nil {
		panic(fmt.Errorf("get metadata, %w", err))
	}

	record := goqu.Record{}
	for _, col := range md.Columns {
		if !col.RefuseUpdate {
			record[col.DBField] = goqu.I(fmt.Sprintf("EXCLUDED.%s", col.DBField))
		}
	}

	for _, col := range otherColumns {
		record[col] = goqu.I(fmt.Sprintf("EXCLUDED.%s", col))
	}

	return record
}

// NewUpsertTarget build "INSERT ... ON CONFLICT target DO UPDATE" target string based on primary keys
func NewUpsertTarget(ent Entity) string {
	md, err := getMetadata(ent)
	if err != nil {
		panic(fmt.Errorf("get metadata, %w", err))
	}

	keys := md.PrimaryKeys
	if len(keys) == 1 {
		return keys[0].DBField
	}

	target := make([]string, 0, len(keys))
	for _, key := range keys {
		target = append(target, key.DBField)
	}
	return strings.Join(target, ", ")
}

// Pagination contains pagination calculation information for database queries.
type Pagination struct {
	First    int `json:"first"`
	Last     int `json:"last"`
	Previous int `json:"previous"`
	Current  int `json:"current"`
	Next     int `json:"next"`
	Size     int `json:"size"`
	Items    int `json:"items"`
}

// NewPagination calculates and returns pagination information based on current page, page size, and total items.
func NewPagination(current, size, items int) Pagination {
	if current <= 0 {
		current = 1
	}
	if size <= 0 {
		size = 10
	}

	p := Pagination{
		Size:    size,
		First:   1,
		Last:    1,
		Current: current,
	}

	if items > 0 {
		p.Items = items
		p.Last = int(math.Ceil(float64(p.Items) / float64(p.Size)))
	}

	if p.Current < p.First {
		p.Current = p.First
	} else if p.Current > p.Last {
		p.Current = p.Last
	}

	if p.Current > p.First {
		p.Previous = p.Current - 1
	}
	if p.Current < p.Last {
		p.Next = p.Current + 1
	}

	return p
}

// Limit returns the LIMIT value for database queries.
func (p Pagination) Limit() int {
	return p.Size
}

// ULimit returns the LIMIT value as an unsigned integer for database queries.
func (p Pagination) ULimit() uint {
	return uint(p.Size)
}

// Offset returns the OFFSET value for database queries.
func (p Pagination) Offset() int {
	return (p.Current - 1) * p.Size
}

// UOffset returns the OFFSET value as an unsigned integer for database queries.
func (p Pagination) UOffset() uint {
	return uint(p.Offset())
}

// IsNotFound checks whether an error is a not found error.
// Repository returns ErrNotFound when a record is not found.
// GetRecord returns sql.ErrNoRows when a record is not found.
// Use this function to unify error checking.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, sql.ErrNoRows) ||
		errors.Is(err, ErrNotFound)
}
