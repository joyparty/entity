package entity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"

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
//
// Deprecated: Use TransactionX() instead.
func Transaction[T Tx, U TxInitiator[T]](db U, fn func(db DB) error) (err error) {
	return TransactionX(context.Background(), db, fn)
}

// TransactionX 执行事务过程，根据结果选择提交或回滚
func TransactionX[T Tx, U TxInitiator[T]](ctx context.Context, db U, fn func(db DB) error) (err error) {
	return runTransaction(ctx, db, nil, fn)
}

// TransactionWithOptions 执行事务过程，根据结果选择提交或回滚
//
// Deprecated: Use TransactionWithOptionsX() instead.
func TransactionWithOptions[T Tx, U TxInitiator[T]](db U, opt *sql.TxOptions, fn func(db DB) error) (err error) {
	return TransactionWithOptionsX(context.Background(), db, opt, fn)
}

// TransactionWithOptionsX 执行事务过程，根据结果选择提交或回滚
func TransactionWithOptionsX[T Tx, U TxInitiator[T]](ctx context.Context, db U, opt *sql.TxOptions, fn func(db DB) error) (err error) {
	return runTransaction(ctx, db, opt, fn)
}

// TryTransaction 尝试执行事务，如果DB是Tx类型，则直接执行fn，如果DB是TxInitiator类型，则开启事务执行fn
//
// Deprecated: Use TryTransactionX() instead.
func TryTransaction[T Tx](db DB, fn func(db DB) error) error {
	return TryTransactionX[T](context.Background(), db, fn)
}

// TryTransactionX 尝试执行事务，如果DB是Tx类型，则直接执行fn，如果DB是TxInitiator类型，则开启事务执行fn
//
// 由于入参是DB接口，无法直接推导出具体的Tx类型，所以需要在调用时显式指定Tx类型参数
//
// TryTransactionX[*sqlx.Tx](ctx, db, func(db entity.DB) error
func TryTransactionX[T Tx](ctx context.Context, db DB, fn func(db DB) error) error {
	if v, ok := db.(T); ok {
		return fn(v)
	} else if v, ok := db.(TxInitiator[T]); ok {
		return TransactionX(ctx, v, fn)
	}

	var x T
	return fmt.Errorf("db is neither %T nor TxInitiator[%T]", x, x)
}

// TryTransactionWithOptions 尝试执行事务，如果DB不是*sqlx.DB，则直接执行fn，如果DB是TxInitiator类型，则开启事务执行fn
//
// Deprecated: Use TryTransactionWithOptionsX() instead.
func TryTransactionWithOptions[T Tx](db DB, opt *sql.TxOptions, fn func(db DB) error) error {
	return TryTransactionWithOptionsX[T](context.Background(), db, opt, fn)
}

// TryTransactionWithOptionsX 尝试执行事务，如果DB是Tx类型，则直接执行fn，如果DB是TxInitiator类型，则开启事务执行fn
//
// 由于入参是DB接口，无法直接推导出具体的Tx类型，所以需要在调用时显式指定Tx类型参数
//
// TryTransactionWithOptionsX[*sqlx.Tx](ctx, db, opt, func(db entity.DB) error
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

// TrySavePoint 尝试创建保存点，如果db不是Tx类型，则返回错误
func TrySavePoint(ctx context.Context, db DB, name string, fn func() error) error {
	if v, ok := db.(Tx); ok {
		return SavePoint(ctx, v, name, fn)
	}

	return errors.New("is not in transaction")
}

var validSavePointName = regexp.MustCompile(`^[0-9a-zA-Z_]+$`)

// SavePoint 在事务中创建保存点，并在fn执行成功后释放保存点，fn执行失败或panic时回滚到保存点
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

// NewUpsertRecord 构建upsert更新的记录
//
// 凡是refuse update的字段都不会被更新，如果需要更新其他字段，可以通过columns参数指定
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

// IsNotFound 判断是否是未找到错误
//
// repository在没有找到记录时返回ErrNotFound错误
// GetRecord()在没有找到记录时返回sql.ErrNoRows错误
// 使用这个方法来统一处理错误判断
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, sql.ErrNoRows) ||
		errors.Is(err, ErrNotFound)
}
