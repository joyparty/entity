package entity

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
)

// 封装了一些goqu的快捷调用

// ExecInsert 执行插入语句
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

// ExecUpdate 执行更新语句
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

// ExecDelete 执行删除语句
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

// GetRecord 执行查询语句，返回单条结果
func GetRecord(ctx context.Context, dest interface{}, db DB, stmt *goqu.SelectDataset) error {
	if !stmt.IsPrepared() {
		stmt = stmt.Prepared(true)
	}

	query, args, err := stmt.ToSQL()
	if err != nil {
		return fmt.Errorf("build select statement, %w", err)
	}
	return db.GetContext(ctx, dest, query, args...)
}

// GetRecords 执行查询语句，返回多条结果
func GetRecords(ctx context.Context, dest interface{}, db DB, stmt *goqu.SelectDataset) error {
	if !stmt.IsPrepared() {
		stmt = stmt.Prepared(true)
	}

	query, args, err := stmt.ToSQL()
	if err != nil {
		return fmt.Errorf("build select statement, %w", err)
	}
	return db.SelectContext(ctx, dest, query, args...)
}

// GetTotalCount 符合条件的总记录数量
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

// Transaction 执行事务过程，根据结果选择提交或回滚
func Transaction(db *sqlx.DB, fn func(tx *sqlx.Tx) error) (err error) {
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("begin transaction, %w", err)
	}

	defer func() {
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

// TryTransaction 尝试执行事务，如果DB不是*sqlx.DB，则直接执行fn
func TryTransaction(db DB, fn func(DB) error) error {
	if v, ok := db.(*sqlx.DB); ok {
		return Transaction(v, func(tx *sqlx.Tx) error {
			return fn(tx)
		})
	}
	return fn(db)
}

// QueryBy 查询并使用回调函数处理游标
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

// Pagination 数据库分页计算
type Pagination struct {
	First    int `json:"first"`
	Last     int `json:"last"`
	Previous int `json:"previous"`
	Current  int `json:"current"`
	Next     int `json:"next"`
	Size     int `json:"size"`
	Items    int `json:"items"`
}

// NewPagination 计算分页页码
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

// Limit 数据库查询LIMIT值
func (p Pagination) Limit() int {
	return p.Size
}

// ULimit 数据库查询LIMIT值
func (p Pagination) ULimit() uint {
	return uint(p.Size)
}

// Offset 数据库查询OFFSET值
func (p Pagination) Offset() int {
	return (p.Current - 1) * p.Size
}

// UOffset 数据库查询OFFSET值
func (p Pagination) UOffset() uint {
	return uint(p.Offset())
}
