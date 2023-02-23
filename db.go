package entity

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
)

const (
	commandSelect = "select"
	commandInsert = "insert"
	commandUpdate = "update"
	commandDelete = "delete"

	driverMysql    = "mysql"
	driverPostgres = "postgres"
	driverSqlite3  = "sqlite3"
)

var (
	selectStatements = &sync.Map{}
	insertStatements = &sync.Map{}
	updateStatements = &sync.Map{}
	deleteStatements = &sync.Map{}

	driverAlias = map[string]string{
		"pgx":    driverPostgres,
		"sqlite": driverSqlite3,
	}

	// interface assert
	_ DB = (*sqlx.DB)(nil)
	_ DB = (*sqlx.Tx)(nil)
)

// DB 数据库接口
// sqlx.DB 和 sqlx.Tx 公共方法
type DB interface {
	sqlx.Queryer
	sqlx.QueryerContext
	sqlx.Execer
	sqlx.ExecerContext
	sqlx.Preparer
	sqlx.PreparerContext
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
	PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)
	Preparex(query string) (*sqlx.Stmt, error)
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	DriverName() string
	Rebind(string) string
	BindNamed(string, interface{}) (string, []interface{}, error)
}

func dbDriver(db DB) string {
	dv := db.DriverName()
	if v, ok := driverAlias[dv]; ok {
		return v
	}
	return dv
}

func isConflictError(err error, driver string) bool {
	s := err.Error()
	if driver == driverPostgres {
		return strings.Contains(s, "duplicate key value violates unique constraint")
	} else if driver == driverMysql {
		return strings.Contains(s, "Duplicate entry")
	} else if driver == driverSqlite3 {
		return strings.Contains(s, "UNIQUE constraint failed")
	}
	return false
}

func doLoad(ctx context.Context, ent Entity, db DB) error {
	md, err := getMetadata(ent)
	if err != nil {
		return fmt.Errorf("get metadata, %w", err)
	}

	stmt := getStatement(commandSelect, md, dbDriver(db))
	rows, err := sqlx.NamedQueryContext(ctx, db, stmt, ent)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return sql.ErrNoRows
	}

	if err := rows.StructScan(ent); err != nil {
		return fmt.Errorf("scan struct, %w", err)
	}

	return rows.Err()
}

func doInsert(ctx context.Context, ent Entity, db DB) (int64, error) {
	md, err := getMetadata(ent)
	if err != nil {
		return 0, fmt.Errorf("get metadata, %w", err)
	}

	stmt := getStatement(commandInsert, md, dbDriver(db))
	if md.hasReturningInsert {
		rows, err := sqlx.NamedQueryContext(ctx, db, stmt, ent)
		if err != nil {
			return 0, err
		}
		defer rows.Close()

		if !rows.Next() {
			return 0, sql.ErrNoRows
		}

		if err := rows.StructScan(ent); err != nil {
			return 0, fmt.Errorf("scan struct, %w", err)
		}

		return 0, rows.Err()
	}

	result, err := db.NamedExecContext(ctx, stmt, ent)
	if err != nil {
		return 0, err
	}

	// postgresql不支持LastInsertId特性
	if dbDriver(db) == driverPostgres {
		return 0, nil
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get last insert id, %w", err)
	}
	return lastID, nil
}

func doUpdate(ctx context.Context, ent Entity, db DB) error {
	md, err := getMetadata(ent)
	if err != nil {
		return fmt.Errorf("get metadata, %w", err)
	}

	stmt := getStatement(commandUpdate, md, dbDriver(db))
	if md.hasReturningUpdate {
		rows, err := sqlx.NamedQueryContext(ctx, db, stmt, ent)
		if err != nil {
			return err
		}
		defer rows.Close()

		if !rows.Next() {
			return sql.ErrNoRows
		}

		if err := rows.StructScan(ent); err != nil {
			return fmt.Errorf("scan struct, %w", err)
		}

		return rows.Err()
	}

	_, err = db.NamedExecContext(ctx, stmt, ent)
	return err
}

func doDelete(ctx context.Context, ent Entity, db DB) error {
	md, err := getMetadata(ent)
	if err != nil {
		return fmt.Errorf("get metadata, %w", err)
	}

	stmt := getStatement(commandDelete, md, dbDriver(db))
	_, err = db.NamedExecContext(ctx, stmt, ent)
	return err
}

func getStatement(cmd string, md *Metadata, driver string) string {
	var (
		m  *sync.Map
		fn func(*Metadata, string) string
	)

	switch cmd {
	case commandSelect:
		m = selectStatements
		fn = newSelectStatement
	case commandInsert:
		m = insertStatements
		fn = newInsertStatement
	case commandUpdate:
		m = updateStatements
		fn = newUpdateStatement
	case commandDelete:
		m = deleteStatements
		fn = newDeleteStatement
	default:
		panic(fmt.Errorf("unimplemented command %q", cmd))
	}

	if v, ok := m.Load(md.Type); ok {
		return v.(string)
	}

	stmt := fn(md, driver)
	m.Store(md.Type, stmt)
	return stmt
}

func newSelectStatement(md *Metadata, driver string) string {
	columns := []string{}
	for _, col := range md.Columns {
		columns = append(columns, quoteColumn(col.DBField, driver))
	}
	stmt := fmt.Sprintf("SELECT %s FROM %s WHERE", strings.Join(columns, ", "), quoteIdentifier(md.TableName, driver))

	for i, col := range md.PrimaryKeys {
		if i == 0 {
			stmt += fmt.Sprintf(" %s = :%s", quoteColumn(col.DBField, driver), col.DBField)
		} else {
			stmt += fmt.Sprintf(" AND %s = :%s", quoteColumn(col.DBField, driver), col.DBField)
		}
	}
	stmt += " LIMIT 1"

	return stmt
}

func newInsertStatement(md *Metadata, driver string) string {
	columns := []string{}
	returnings := []string{}
	placeholder := []string{}

	for _, col := range md.Columns {
		c := quoteColumn(col.DBField, driver)
		if col.ReturningInsert {
			returnings = append(returnings, c)
		} else if !col.AutoIncrement {
			columns = append(columns, c)
			placeholder = append(placeholder, fmt.Sprintf(":%s", col.DBField))
		}
	}

	stmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(md.TableName, driver),
		strings.Join(columns, ", "),
		strings.Join(placeholder, ", "),
	)

	if len(returnings) > 0 {
		stmt += fmt.Sprintf(" RETURNING %s", strings.Join(returnings, ", "))
	}

	return stmt
}

func newUpdateStatement(md *Metadata, driver string) string {
	returnings := []string{}
	stmt := fmt.Sprintf("UPDATE %s SET", quoteIdentifier(md.TableName, driver))

	set := false
	for _, col := range md.Columns {
		if col.ReturningUpdate {
			returnings = append(returnings, quoteColumn(col.DBField, driver))
		} else if !col.RefuseUpdate {
			if set {
				stmt += fmt.Sprintf(", %s = :%s", quoteColumn(col.DBField, driver), col.DBField)
			} else {
				stmt += fmt.Sprintf(" %s = :%s", quoteColumn(col.DBField, driver), col.DBField)
				set = true
			}
		}
	}

	for i, col := range md.PrimaryKeys {
		if i == 0 {
			stmt += fmt.Sprintf(" WHERE %s = :%s", quoteColumn(col.DBField, driver), col.DBField)
		} else {
			stmt += fmt.Sprintf(" AND %s = :%s", quoteColumn(col.DBField, driver), col.DBField)
		}
	}

	if len(returnings) > 0 {
		stmt += fmt.Sprintf(" RETURNING %s", strings.Join(returnings, ", "))
	}

	return stmt
}

func newDeleteStatement(md *Metadata, driver string) string {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE", quoteIdentifier(md.TableName, driver))
	for i, col := range md.PrimaryKeys {
		if i == 0 {
			stmt += fmt.Sprintf(" %s = :%s", quoteColumn(col.DBField, driver), col.DBField)
		} else {
			stmt += fmt.Sprintf(" AND %s = :%s", quoteColumn(col.DBField, driver), col.DBField)
		}
	}

	return stmt
}

func quoteColumn(name string, driver string) string {
	if driver == driverMysql {
		return fmt.Sprintf("`%s`", name)
	}
	return fmt.Sprintf("%q", name)
}

func quoteIdentifier(name string, driver string) string {
	symbol := `"`
	if driver == driverMysql {
		symbol = "`"
	}

	result := []string{}
	name = strings.ReplaceAll(name, symbol, "")
	for _, s := range strings.Split(name, ".") {
		if s != "*" {
			s = fmt.Sprintf("%s%s%s", symbol, s, symbol)
		}
		result = append(result, s)
	}

	return strings.Join(result, ".")
}
