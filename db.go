package entity

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

var (
	selectStatements = map[string]string{}
	insertStatements = map[string]string{}
	updateStatements = map[string]string{}
	deleteStatements = map[string]string{}

	dialects = map[string]*dialect{}
)

// DB 数据库接口
// sqlx.DB 和 sqlx.Tx 公共方法
type DB interface {
	sqlx.Queryer
	sqlx.QueryerContext
	sqlx.Execer
	sqlx.ExecerContext
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	DriverName() string
	Rebind(string) string
	BindNamed(string, interface{}) (string, []interface{}, error)
}

// dialect 数据库特性
type dialect struct {
	Driver    string
	Returning bool
}

func getDialect(db DB) *dialect {
	driver := db.DriverName()
	if driver == "pgx" {
		driver = "postgres"
	}

	if v, ok := dialects[driver]; ok {
		return v
	}

	dia := &dialect{Driver: driver}
	if dia.Driver == "postgres" {
		dia.Returning = true
	}

	dialects[dia.Driver] = dia
	return dia
}

// Load 从数据库载入entity
func Load(ctx context.Context, entity Entity, db DB) error {
	ctx, cancel := context.WithTimeout(ctx, ReadTimeout)
	defer cancel()

	cv, cacheable := entity.(Cacheable)
	if cacheable {
		if loaded, err := loadCache(cv); err != nil {
			return errors.WithMessage(err, "load entity from cache")
		} else if loaded {
			return nil
		}
	}

	if err := load(ctx, entity, db); err != nil {
		return errors.WithMessage(err, "load entity from db")
	}

	if cacheable {
		if err := SaveCache(cv); err != nil {
			return errors.WithMessage(err, "found entity")
		}
	}

	return nil
}

func load(ctx context.Context, entity Entity, db DB) error {
	md, err := getMetadata(entity)
	if err != nil {
		return err
	}

	stmt, ok := selectStatements[md.ID]
	if !ok {
		stmt = selectStatement(entity, md, getDialect(db))
		selectStatements[md.ID] = stmt
	}

	rows, err := sqlx.NamedQueryContext(ctx, db, stmt, entity)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return errors.WithStack(sql.ErrNoRows)
	}

	if err := rows.StructScan(entity); err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(rows.Err())
}

// Insert 插入新entity
func Insert(ctx context.Context, entity Entity, db DB) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := entity.OnEntityEvent(ctx, EventBeforeInsert); err != nil {
		return 0, errors.WithMessage(err, "before insert entity")
	}

	lastID, err := insert(ctx, entity, db)
	if err != nil {
		return 0, errors.WithMessage(err, "insert entity")
	}

	if err := entity.OnEntityEvent(ctx, EventAfterInsert); err != nil {
		return 0, errors.WithMessage(err, "after insert entity")
	}

	return lastID, nil
}

func insert(ctx context.Context, entity Entity, db DB) (int64, error) {
	md, err := getMetadata(entity)
	if err != nil {
		return 0, err
	}

	dia := getDialect(db)

	stmt, ok := insertStatements[md.ID]
	if !ok {
		stmt = insertStatement(entity, md, dia)
		insertStatements[md.ID] = stmt
	}

	if dia.Returning && strings.Contains(stmt, ") RETURNING ") {
		rows, err := sqlx.NamedQueryContext(ctx, db, stmt, entity)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		defer rows.Close()

		if !rows.Next() {
			return 0, errors.WithStack(sql.ErrNoRows)
		}

		if err := rows.StructScan(entity); err != nil {
			return 0, errors.WithStack(err)
		}

		return 0, errors.WithStack(rows.Err())
	}

	result, err := db.NamedExecContext(ctx, stmt, entity)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	// postgresql不支持LastInsertId特性
	if db.DriverName() == "pgx" || db.DriverName() == "postgres" {
		return 0, nil
	}

	lastID, err := result.LastInsertId()
	return lastID, errors.WithStack(err)
}

// Update 更新entity
func Update(ctx context.Context, entity Entity, db DB) error {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := entity.OnEntityEvent(ctx, EventBeforeUpdate); err != nil {
		return errors.WithMessage(err, "before update entity")
	}

	if err := update(ctx, entity, db); err != nil {
		return errors.WithMessage(err, "update entity")
	}

	if v, ok := entity.(Cacheable); ok {
		if err := DeleteCache(v); err != nil {
			return errors.WithMessage(err, "after update entity")
		}
	}

	return errors.WithMessage(
		entity.OnEntityEvent(ctx, EventAfterUpdate),
		"after update entity",
	)
}

func update(ctx context.Context, entity Entity, db DB) error {
	md, err := getMetadata(entity)
	if err != nil {
		return err
	}

	dia := getDialect(db)

	stmt, ok := updateStatements[md.ID]
	if !ok {
		stmt = updateStatement(entity, md, dia)
		updateStatements[md.ID] = stmt
	}

	if dia.Returning && strings.Contains(stmt, " RETURNING ") {
		rows, err := sqlx.NamedQueryContext(ctx, db, stmt, entity)
		if err != nil {
			return errors.WithStack(err)
		}
		defer rows.Close()

		if !rows.Next() {
			return errors.WithStack(sql.ErrNoRows)
		}

		if err := rows.StructScan(entity); err != nil {
			return errors.WithStack(err)
		}

		return errors.WithStack(rows.Err())
	}

	result, err := db.NamedExecContext(ctx, stmt, entity)
	if err != nil {
		return errors.WithStack(err)
	}

	if n, err := result.RowsAffected(); err != nil {
		return errors.WithStack(err)
	} else if n == 0 {
		return errors.WithStack(sql.ErrNoRows)
	}

	return nil
}

// Delete 删除entity
func Delete(ctx context.Context, entity Entity, db DB) error {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := entity.OnEntityEvent(ctx, EventBeforeDelete); err != nil {
		return err
	}

	if err := _delete(ctx, entity, db); err != nil {
		return err
	}

	if v, ok := entity.(Cacheable); ok {
		if err := DeleteCache(v); err != nil {
			return errors.WithMessage(err, "after delete entity")
		}
	}

	return errors.WithMessage(
		entity.OnEntityEvent(ctx, EventAfterDelete),
		"after delete entity",
	)
}

func _delete(ctx context.Context, entity Entity, db DB) error {
	md, err := getMetadata(entity)
	if err != nil {
		return errors.WithMessage(err, "delete entity")
	}

	stmt, ok := deleteStatements[md.ID]
	if !ok {
		stmt = deleteStatement(entity, md, getDialect(db))
		deleteStatements[md.ID] = stmt
	}

	_, err = db.NamedExecContext(ctx, stmt, entity)
	return errors.Wrapf(err, "delete entity %s", md.ID)
}

func selectStatement(entity Entity, md *Metadata, dia *dialect) string {
	columns := []string{}
	for _, col := range md.Columns {
		columns = append(columns, quoteColumn(col.DBField, dia))
	}
	stmt := fmt.Sprintf("SELECT %s FROM %s WHERE", strings.Join(columns, ", "), md.TableName)

	for i, col := range md.PrimaryKeys {
		if i == 0 {
			stmt += fmt.Sprintf(" %s = :%s", quoteColumn(col.DBField, dia), col.DBField)
		} else {
			stmt += fmt.Sprintf(" AND %s = :%s", quoteColumn(col.DBField, dia), col.DBField)
		}
	}
	stmt += " LIMIT 1"

	return stmt
}

func insertStatement(entity Entity, md *Metadata, dia *dialect) string {
	columns := []string{}
	returnings := []string{}
	placeholder := []string{}

	for _, col := range md.Columns {
		c := quoteColumn(col.DBField, dia)
		returing := dia.Returning && col.Returning
		if returing {
			returnings = append(returnings, c)
		}

		if col.AutoIncrement {
			continue
		}

		if !returing {
			columns = append(columns, c)
			placeholder = append(placeholder, fmt.Sprintf(":%s", col.DBField))
		}
	}

	stmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		md.TableName,
		strings.Join(columns, ", "),
		strings.Join(placeholder, ", "),
	)

	if dia.Returning && len(returnings) > 0 {
		stmt += fmt.Sprintf(" RETURNING %s", strings.Join(returnings, ", "))
	}

	return stmt
}

func updateStatement(entity Entity, md *Metadata, dia *dialect) string {
	returnings := []string{}
	stmt := fmt.Sprintf("UPDATE %s SET", md.TableName)

	set := false
	for _, col := range md.Columns {
		if col.RefuseUpdate {
			continue
		}

		if dia.Returning && col.Returning {
			returnings = append(returnings, quoteColumn(col.DBField, dia))
		} else {
			if set {
				stmt += fmt.Sprintf(", %s = :%s", quoteColumn(col.DBField, dia), col.DBField)
			} else {
				stmt += fmt.Sprintf(" %s = :%s", quoteColumn(col.DBField, dia), col.DBField)
				set = true
			}
		}
	}

	for i, col := range md.PrimaryKeys {
		if i == 0 {
			stmt += fmt.Sprintf(" WHERE %s = :%s", quoteColumn(col.DBField, dia), col.DBField)
		} else {
			stmt += fmt.Sprintf(" AND %s = :%s", quoteColumn(col.DBField, dia), col.DBField)
		}
	}

	if dia.Returning && len(returnings) > 0 {
		stmt += fmt.Sprintf(" RETURNING %s", strings.Join(returnings, ", "))
	}

	return stmt
}

func deleteStatement(entity Entity, md *Metadata, dia *dialect) string {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE", md.TableName)
	for i, col := range md.PrimaryKeys {
		if i == 0 {
			stmt += fmt.Sprintf(" %s = :%s", quoteColumn(col.DBField, dia), col.DBField)
		} else {
			stmt += fmt.Sprintf(" AND %s = :%s", quoteColumn(col.DBField, dia), col.DBField)
		}
	}

	return stmt
}

func quoteColumn(name string, dia *dialect) string {
	dn := dia.Driver
	if dn == "postgres" {
		return fmt.Sprintf("%q", name)
	}

	return fmt.Sprintf("`%s`", name)
}
