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

func isConflictError(driver string, err error) bool {
	s := errors.Cause(err).Error()
	if isPostgres(driver) {
		return strings.Contains(s, "duplicate key value violates unique constraint")
	} else if driver == "mysql" {
		return strings.Contains(s, "Duplicate entry")
	} else if driver == "sqlite3" {
		return strings.Contains(s, "UNIQUE constraint failed")
	}
	return false
}

func isPostgres(driver string) bool {
	return driver == "pgx" || driver == "postgres"
}

func doLoad(ctx context.Context, ent Entity, db DB) error {
	md, err := getMetadata(ent)
	if err != nil {
		return err
	}

	stmt, ok := selectStatements[md.ID]
	if !ok {
		stmt = selectStatement(ent, md, db.DriverName())
		selectStatements[md.ID] = stmt
	}

	rows, err := sqlx.NamedQueryContext(ctx, db, stmt, ent)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return errors.WithStack(sql.ErrNoRows)
	}

	if err := rows.StructScan(ent); err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(rows.Err())
}

func doInsert(ctx context.Context, ent Entity, db DB) (int64, error) {
	md, err := getMetadata(ent)
	if err != nil {
		return 0, err
	}

	stmt, ok := insertStatements[md.ID]
	if !ok {
		stmt = insertStatement(ent, md, db.DriverName())
		insertStatements[md.ID] = stmt
	}

	if md.hasReturningInsert {
		rows, err := sqlx.NamedQueryContext(ctx, db, stmt, ent)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		defer rows.Close()

		if !rows.Next() {
			return 0, errors.WithStack(sql.ErrNoRows)
		}

		if err := rows.StructScan(ent); err != nil {
			return 0, errors.WithStack(err)
		}

		return 0, errors.WithStack(rows.Err())
	}

	result, err := db.NamedExecContext(ctx, stmt, ent)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	// postgresql不支持LastInsertId特性
	if isPostgres(db.DriverName()) {
		return 0, nil
	}

	lastID, err := result.LastInsertId()
	return lastID, errors.WithStack(err)
}

func doUpdate(ctx context.Context, ent Entity, db DB) error {
	md, err := getMetadata(ent)
	if err != nil {
		return err
	}

	stmt, ok := updateStatements[md.ID]
	if !ok {
		stmt = updateStatement(ent, md, db.DriverName())
		updateStatements[md.ID] = stmt
	}

	if md.hasReturningUpdate {
		rows, err := sqlx.NamedQueryContext(ctx, db, stmt, ent)
		if err != nil {
			return errors.WithStack(err)
		}
		defer rows.Close()

		if !rows.Next() {
			return errors.WithStack(sql.ErrNoRows)
		}

		if err := rows.StructScan(ent); err != nil {
			return errors.WithStack(err)
		}

		return errors.WithStack(rows.Err())
	}

	result, err := db.NamedExecContext(ctx, stmt, ent)
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

func doDelete(ctx context.Context, ent Entity, db DB) error {
	md, err := getMetadata(ent)
	if err != nil {
		return errors.WithMessage(err, "delete entity")
	}

	stmt, ok := deleteStatements[md.ID]
	if !ok {
		stmt = deleteStatement(ent, md, db.DriverName())
		deleteStatements[md.ID] = stmt
	}

	_, err = db.NamedExecContext(ctx, stmt, ent)
	return errors.Wrapf(err, "delete entity %s", md.ID)
}

func selectStatement(ent Entity, md *Metadata, dirver string) string {
	columns := []string{}
	for _, col := range md.Columns {
		columns = append(columns, quoteColumn(col.DBField, dirver))
	}
	stmt := fmt.Sprintf("SELECT %s FROM %s WHERE", strings.Join(columns, ", "), md.TableName)

	for i, col := range md.PrimaryKeys {
		if i == 0 {
			stmt += fmt.Sprintf(" %s = :%s", quoteColumn(col.DBField, dirver), col.DBField)
		} else {
			stmt += fmt.Sprintf(" AND %s = :%s", quoteColumn(col.DBField, dirver), col.DBField)
		}
	}
	stmt += " LIMIT 1"

	return stmt
}

func insertStatement(ent Entity, md *Metadata, dirver string) string {
	columns := []string{}
	returnings := []string{}
	placeholder := []string{}

	for _, col := range md.Columns {
		c := quoteColumn(col.DBField, dirver)
		if col.ReturningInsert {
			returnings = append(returnings, c)
		} else if !col.AutoIncrement {
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

	if len(returnings) > 0 {
		stmt += fmt.Sprintf(" RETURNING %s", strings.Join(returnings, ", "))
	}

	return stmt
}

func updateStatement(ent Entity, md *Metadata, dirver string) string {
	returnings := []string{}
	stmt := fmt.Sprintf("UPDATE %s SET", md.TableName)

	set := false
	for _, col := range md.Columns {
		if col.ReturningUpdate {
			returnings = append(returnings, quoteColumn(col.DBField, dirver))
		} else if !col.RefuseUpdate {
			if set {
				stmt += fmt.Sprintf(", %s = :%s", quoteColumn(col.DBField, dirver), col.DBField)
			} else {
				stmt += fmt.Sprintf(" %s = :%s", quoteColumn(col.DBField, dirver), col.DBField)
				set = true
			}
		}
	}

	for i, col := range md.PrimaryKeys {
		if i == 0 {
			stmt += fmt.Sprintf(" WHERE %s = :%s", quoteColumn(col.DBField, dirver), col.DBField)
		} else {
			stmt += fmt.Sprintf(" AND %s = :%s", quoteColumn(col.DBField, dirver), col.DBField)
		}
	}

	if len(returnings) > 0 {
		stmt += fmt.Sprintf(" RETURNING %s", strings.Join(returnings, ", "))
	}

	return stmt
}

func deleteStatement(ent Entity, md *Metadata, dirver string) string {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE", md.TableName)
	for i, col := range md.PrimaryKeys {
		if i == 0 {
			stmt += fmt.Sprintf(" %s = :%s", quoteColumn(col.DBField, dirver), col.DBField)
		} else {
			stmt += fmt.Sprintf(" AND %s = :%s", quoteColumn(col.DBField, dirver), col.DBField)
		}
	}

	return stmt
}

func quoteColumn(name string, dirver string) string {
	if dirver == "mysql" {
		return fmt.Sprintf("`%s`", name)
	}

	return fmt.Sprintf("%q", name)
}
