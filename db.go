package entity

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

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
	selectStatements = map[reflect.Type]string{}
	insertStatements = map[reflect.Type]string{}
	updateStatements = map[reflect.Type]string{}
	deleteStatements = map[reflect.Type]string{}

	driverAlias = map[string]string{
		"pgx": driverPostgres,
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

	result, err := db.NamedExecContext(ctx, stmt, ent)
	if err != nil {
		return err
	}

	if n, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("get affected rows, %w", err)
	} else if n == 0 {
		return sql.ErrNoRows
	}

	return nil
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
		m  map[reflect.Type]string
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

	if stmt, ok := m[md.Type]; ok {
		return stmt
	}

	stmt := fn(md, driver)
	m[md.Type] = stmt
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

// PrepareInsertStatement is a prepared insert statement for entity
type PrepareInsertStatement struct {
	md       *Metadata
	stmt     *sqlx.NamedStmt
	dbDriver string
}

// Close closes the prepared statement
func (pis *PrepareInsertStatement) Close() error {
	return pis.stmt.Close()
}

// ExecContext executes a prepared insert statement using the Entity passed.
func (pis *PrepareInsertStatement) ExecContext(ctx context.Context, ent Entity) (lastID int64, err error) {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := ent.OnEntityEvent(ctx, EventBeforeInsert); err != nil {
		return 0, fmt.Errorf("before insert, %w", err)
	}

	lastID, err = pis.execContext(ctx, ent)
	if err != nil {
		if isConflictError(err, pis.dbDriver) {
			return 0, ErrConflict
		}
		return 0, err
	}

	if err := ent.OnEntityEvent(ctx, EventAfterInsert); err != nil {
		return 0, fmt.Errorf("after insert, %w", err)
	}

	return lastID, nil
}

func (pis *PrepareInsertStatement) execContext(ctx context.Context, ent Entity) (lastID int64, err error) {
	if pis.md.hasReturningInsert {
		err := pis.stmt.QueryRowxContext(ctx, ent).StructScan(ent)
		return 0, err
	}

	result, err := pis.stmt.ExecContext(ctx, ent)
	if err != nil {
		return 0, err
	} else if pis.dbDriver == driverPostgres {
		// postgresql不支持LastInsertId特性
		return 0, nil
	}

	lastID, err = result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get last insert id, %w", err)
	}
	return lastID, nil
}

// PrepareUpdateStatement is a prepared update statement for entity
type PrepareUpdateStatement struct {
	md       *Metadata
	stmt     *sqlx.NamedStmt
	dbDriver string
}

// Close closes the prepared statement
func (pus *PrepareUpdateStatement) Close() error {
	return pus.stmt.Close()
}

// ExecContext executes a prepared update statement using the Entity passed.
func (pus *PrepareUpdateStatement) ExecContext(ctx context.Context, ent Entity) error {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := ent.OnEntityEvent(ctx, EventBeforeUpdate); err != nil {
		return fmt.Errorf("before update, %w", err)
	}

	if err := pus.execContext(ctx, ent); err != nil {
		return err
	}

	if v, ok := ent.(Cacheable); ok {
		if err := DeleteCache(v); err != nil {
			return fmt.Errorf("delete cache, %w", err)
		}
	}

	if err := ent.OnEntityEvent(ctx, EventAfterUpdate); err != nil {
		return fmt.Errorf("after update, %w", err)
	}
	return nil
}

func (pus *PrepareUpdateStatement) execContext(ctx context.Context, ent Entity) error {
	if pus.md.hasReturningUpdate {
		return pus.stmt.QueryRowxContext(ctx, ent).StructScan(ent)
	}

	result, err := pus.stmt.ExecContext(ctx, ent)
	if err != nil {
		return err
	}

	if n, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("get affected rows, %w", err)
	} else if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}
