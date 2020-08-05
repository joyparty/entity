package entity

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/doug-martin/goqu/v9"
)

// 封装了一些goqu的快捷调用

// ExecUpdate 执行更新语句
func ExecUpdate(ctx context.Context, db DB, stmt *goqu.UpdateDataset) (sql.Result, error) {
	query, args, err := stmt.Prepared(true).ToSQL()
	if err != nil {
		return nil, fmt.Errorf("build update statement, %w", err)
	}
	return db.ExecContext(ctx, query, args...)
}

// ExecDelete 执行删除语句
func ExecDelete(ctx context.Context, db DB, stmt *goqu.DeleteDataset) (sql.Result, error) {
	query, args, err := stmt.Prepared(true).ToSQL()
	if err != nil {
		return nil, fmt.Errorf("build delete statement, %w", err)
	}
	return db.ExecContext(ctx, query, args...)
}

// GetRecode 执行查询语句，返回单条结果
func GetRecode(ctx context.Context, dest interface{}, db DB, stmt *goqu.SelectDataset) error {
	query, args, err := stmt.Prepared(true).ToSQL()
	if err != nil {
		return fmt.Errorf("build select statement, %w", err)
	}
	return db.GetContext(ctx, dest, query, args...)
}

// GetRecodes 执行查询语句，返回多条结果
func GetRecodes(ctx context.Context, dest interface{}, db DB, stmt *goqu.SelectDataset) error {
	query, args, err := stmt.Prepared(true).ToSQL()
	if err != nil {
		return fmt.Errorf("build select statement, %w", err)
	}
	return db.SelectContext(ctx, dest, query, args...)
}

// GetTotalCount 符合条件的总记录数量
func GetTotalCount(ctx context.Context, db DB, stmt *goqu.SelectDataset) (int, error) {
	stmt = stmt.
		Select(goqu.L(`count(1)`)).
		ClearOrder().
		ClearLimit().
		ClearOffset()

	var total int
	if err := GetRecode(ctx, &total, db, stmt); err != nil {
		return 0, err
	}
	return total, nil
}
