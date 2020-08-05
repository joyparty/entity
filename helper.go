package entity

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
)

// 封装了一些goqu的快捷调用

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

// GetRecorde 执行查询语句，返回单条结果
func GetRecorde(ctx context.Context, dest interface{}, db DB, stmt *goqu.SelectDataset) error {
	if !stmt.IsPrepared() {
		stmt = stmt.Prepared(true)
	}

	query, args, err := stmt.ToSQL()
	if err != nil {
		return fmt.Errorf("build select statement, %w", err)
	}
	return db.GetContext(ctx, dest, query, args...)
}

// GetRecordes 执行查询语句，返回多条结果
func GetRecordes(ctx context.Context, dest interface{}, db DB, stmt *goqu.SelectDataset) error {
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
	if err := GetRecorde(ctx, &total, db, stmt); err != nil {
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
			if txErr := tx.Commit(); txErr != nil {
				err = fmt.Errorf("commit transaction, %w", txErr)
			}
		} else {
			_ = tx.Rollback()
		}
	}()

	return fn(tx)
}
