// Package entity is an ORM framework based on sqlx.
package entity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
)

const (
	// EventUnknown is an undefined event constant.
	EventUnknown Event = iota
	// EventBeforeInsert before insert entity
	EventBeforeInsert
	// EventAfterInsert after insert entity
	EventAfterInsert
	// EventBeforeUpdate before update entity
	EventBeforeUpdate
	// EventAfterUpdate after update entity
	EventAfterUpdate
	// EventBeforeDelete before delete entity
	EventBeforeDelete
	// EventAfterDelete after delete entity
	EventAfterDelete
)

var (
	// ErrConflict is returned when a data conflict is detected.
	ErrConflict = errors.New("record conflict")

	// ErrNotFound is returned when a record is not found.
	ErrNotFound = errors.New("record not found")

	// ReadTimeout is the default timeout for reading entity data.
	ReadTimeout = 3 * time.Second
	// WriteTimeout is the default timeout for writing entity data.
	WriteTimeout = 3 * time.Second

	entities = &sync.Map{}

	mapper = reflectx.NewMapper("db")
)

// Event represents a storage event type.
type Event int

// Entity is an interface that all entity objects must implement.
type Entity interface {
	TableName() string
}

// EventHook is an interface for event-style hooks.
type EventHook interface {
	OnEntityEvent(ctx context.Context, ev Event) error
}

// BeforeInsertHook is called before inserting an entity.
type BeforeInsertHook interface {
	BeforeInsert(ctx context.Context) error
}

// AfterInsertHook is called after inserting an entity.
type AfterInsertHook interface {
	AfterInsert(ctx context.Context) error
}

// BeforeUpdateHook is called before updating an entity.
type BeforeUpdateHook interface {
	BeforeUpdate(ctx context.Context) error
}

// AfterUpdateHook is called after updating an entity.
type AfterUpdateHook interface {
	AfterUpdate(ctx context.Context) error
}

// BeforeDeleteHook is called before deleting an entity.
type BeforeDeleteHook interface {
	BeforeDelete(ctx context.Context) error
}

// AfterDeleteHook is called after deleting an entity.
type AfterDeleteHook interface {
	AfterDelete(ctx context.Context) error
}

// Column contains field metadata information.
type Column struct {
	StructField     string
	DBField         string
	PrimaryKey      bool
	AutoIncrement   bool
	RefuseUpdate    bool
	ReturningInsert bool
	ReturningUpdate bool
}

func (c Column) String() string {
	return c.DBField
}

// Metadata contains entity metadata such as table name, columns, and primary keys.
type Metadata struct {
	Type        reflect.Type
	TableName   string
	Columns     []Column
	PrimaryKeys []Column

	hasReturningInsert bool
	hasReturningUpdate bool
}

// NewMetadata constructs and returns the metadata for an entity object.
func NewMetadata(ent Entity) (*Metadata, error) {
	columns := getColumns(ent)

	md := &Metadata{
		Type:        reflectx.Deref(reflect.TypeOf(ent)),
		TableName:   ent.TableName(),
		Columns:     columns,
		PrimaryKeys: []Column{},
	}

	if len(md.Columns) == 0 {
		return nil, fmt.Errorf("empty entity %q", md.Type)
	}

	for _, col := range md.Columns {
		if col.ReturningInsert {
			md.hasReturningInsert = true
		}
		if col.ReturningUpdate {
			md.hasReturningUpdate = true
		}
		if col.PrimaryKey {
			md.PrimaryKeys = append(md.PrimaryKeys, col)
		}
	}

	if len(md.PrimaryKeys) == 0 {
		return nil, fmt.Errorf("entity %q primary key not found", md.Type)
	}

	return md, nil
}

func getMetadata(ent Entity) (*Metadata, error) {
	t := reflectx.Deref(reflect.TypeOf(ent))

	if v, ok := entities.Load(t); ok {
		return v.(*Metadata), nil
	}

	md, err := NewMetadata(ent)
	if err != nil {
		return nil, err
	}

	entities.Store(t, md)
	return md, nil
}

func getColumns(ent Entity) []Column {
	cols := []Column{}
	for _, fi := range getFields(ent) {
		col := Column{
			StructField: fi.Field.Name,
			DBField:     fi.Name,
		}

		for key := range fi.Options {
			switch key {
			case "primaryKey", "primary_key":
				col.PrimaryKey = true
				col.RefuseUpdate = true
			case "refuseUpdate", "refuse_update":
				col.RefuseUpdate = true
			case "returning":
				col.ReturningInsert = true
				col.ReturningUpdate = true
				col.RefuseUpdate = true
			case "returningInsert", "returning_insert":
				col.ReturningInsert = true
			case "returningUpdate", "returning_update":
				col.ReturningUpdate = true
				col.RefuseUpdate = true
			case "autoIncrement", "auto_increment":
				col.AutoIncrement = true
				col.RefuseUpdate = true
			}
		}
		cols = append(cols, col)
	}

	return cols
}

// getFields returns all database fields of an entity object, supporting nested structs. Outer fields have higher priority than inner fields.
func getFields(ent Entity) []*reflectx.FieldInfo {
	done := map[string]struct{}{}

	var get func(node *reflectx.FieldInfo) []*reflectx.FieldInfo
	get = func(node *reflectx.FieldInfo) []*reflectx.FieldInfo {
		fields := []*reflectx.FieldInfo{}

		var embedded []*reflectx.FieldInfo
		for _, v := range node.Children {
			if v != nil {
				if v.Embedded {
					embedded = append(embedded, v)
				} else {
					if _, ok := done[v.Name]; !ok {
						done[v.Name] = struct{}{}

						fields = append(fields, v)
					}
				}
			}
		}

		for _, v := range embedded {
			fields = append(fields, get(v)...)
		}

		return fields
	}

	return get(
		mapper.TypeMap(
			reflectx.Deref(reflect.TypeOf(ent)),
		).Tree,
	)
}

// Load retrieves an entity from the database.
func Load(ctx context.Context, ent Entity, db DB) error {
	ctx, cancel := context.WithTimeout(ctx, ReadTimeout)
	defer cancel()

	cv, cacheable := ent.(Cacheable)
	if cacheable {
		if loaded, err := loadCache(ctx, cv); err != nil {
			return fmt.Errorf("load from cache, %w", err)
		} else if loaded {
			return nil
		}
	}

	if err := doLoad(ctx, ent, db); err != nil {
		return err
	}

	if cacheable {
		if err := SaveCache(ctx, cv); err != nil {
			return fmt.Errorf("save cache, %w", err)
		}
	}

	return nil
}

// Insert saves a new entity to the database.
func Insert(ctx context.Context, ent Entity, db DB) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := beforeInsert(ctx, ent); err != nil {
		return 0, fmt.Errorf("before insert, %w", err)
	}

	lastID, err := doInsert(ctx, ent, db)
	if err != nil {
		if isConflictError(err, dbDriver(db)) {
			return 0, ErrConflict
		}
		return 0, err
	}

	if err := afterInsert(ctx, ent); err != nil {
		return 0, fmt.Errorf("after insert, %w", err)
	}
	return lastID, nil
}

// Update updates an existing entity in the database.
func Update(ctx context.Context, ent Entity, db DB) error {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := beforeUpdate(ctx, ent); err != nil {
		return fmt.Errorf("before update, %w", err)
	}

	if err := doUpdate(ctx, ent, db); err != nil {
		if isConflictError(err, dbDriver(db)) {
			return ErrConflict
		}
		return err
	}

	if v, ok := ent.(Cacheable); ok {
		if err := DeleteCache(ctx, v); err != nil {
			return fmt.Errorf("delete cache, %w", err)
		}
	}

	if err := afterUpdate(ctx, ent); err != nil {
		return fmt.Errorf("after update, %w", err)
	}
	return nil
}

// Upsert inserts a new entity or updates an existing one in the database.
func Upsert(ctx context.Context, ent Entity, db DB) error {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := beforeInsert(ctx, ent); err != nil {
		return fmt.Errorf("before upsert, %w", err)
	} else if err := beforeUpdate(ctx, ent); err != nil {
		return fmt.Errorf("before upsert, %w", err)
	}

	if err := doUpsert(ctx, ent, db); err != nil {
		return err
	}

	if v, ok := ent.(Cacheable); ok {
		if err := DeleteCache(ctx, v); err != nil {
			return fmt.Errorf("delete cache, %w", err)
		}
	}

	if err := afterInsert(ctx, ent); err != nil {
		return fmt.Errorf("after upsert, %w", err)
	} else if err := afterUpdate(ctx, ent); err != nil {
		return fmt.Errorf("after upsert, %w", err)
	}

	return nil
}

// Delete removes an entity from the database.
func Delete(ctx context.Context, ent Entity, db DB) error {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := beforeDelete(ctx, ent); err != nil {
		return fmt.Errorf("before delete, %w", err)
	}

	if err := doDelete(ctx, ent, db); err != nil {
		return err
	}

	if v, ok := ent.(Cacheable); ok {
		if err := DeleteCache(ctx, v); err != nil {
			return fmt.Errorf("delete cache, %w", err)
		}
	}

	if err := afterDelete(ctx, ent); err != nil {
		return fmt.Errorf("after delete, %w", err)
	}
	return nil
}

// PrepareInsertStatement is a prepared statement for inserting entities.
type PrepareInsertStatement struct {
	md       *Metadata
	stmt     *sqlx.NamedStmt
	dbDriver string
}

// PrepareInsert creates a prepared statement for inserting entities.
func PrepareInsert(ctx context.Context, ent Entity, db DB) (*PrepareInsertStatement, error) {
	md, err := getMetadata(ent)
	if err != nil {
		return nil, fmt.Errorf("get metadata, %w", err)
	}

	query := getStatement(commandInsert, md, dbDriver(db))
	stmt, err := db.PrepareNamedContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return &PrepareInsertStatement{
		md:       md,
		stmt:     stmt,
		dbDriver: dbDriver(db),
	}, nil
}

// Close closes the prepared statement
func (pis *PrepareInsertStatement) Close() error {
	return pis.stmt.Close()
}

// ExecContext executes the prepared insert statement with the provided entity.
func (pis *PrepareInsertStatement) ExecContext(ctx context.Context, ent Entity) (lastID int64, err error) {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := beforeInsert(ctx, ent); err != nil {
		return 0, fmt.Errorf("before insert, %w", err)
	}

	lastID, err = pis.execContext(ctx, ent)
	if err != nil {
		if isConflictError(err, pis.dbDriver) {
			return 0, ErrConflict
		}
		return 0, err
	}

	if err := afterInsert(ctx, ent); err != nil {
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
		// PostgreSQL does not support the LastInsertId feature.
		return 0, nil
	}

	lastID, err = result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get last insert id, %w", err)
	}
	return lastID, nil
}

// PrepareUpdateStatement is a prepared statement for updating entities.
type PrepareUpdateStatement struct {
	md       *Metadata
	stmt     *sqlx.NamedStmt
	dbDriver string
}

// PrepareUpdate creates a prepared statement for updating entities.
func PrepareUpdate(ctx context.Context, ent Entity, db DB) (*PrepareUpdateStatement, error) {
	md, err := getMetadata(ent)
	if err != nil {
		return nil, fmt.Errorf("get metadata, %w", err)
	}

	driver := dbDriver(db)
	query := getStatement(commandUpdate, md, driver)
	stmt, err := db.PrepareNamedContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return &PrepareUpdateStatement{
		md:       md,
		stmt:     stmt,
		dbDriver: driver,
	}, nil
}

// Close closes the prepared statement
func (pus *PrepareUpdateStatement) Close() error {
	return pus.stmt.Close()
}

// ExecContext executes the prepared update statement with the provided entity.
func (pus *PrepareUpdateStatement) ExecContext(ctx context.Context, ent Entity) error {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := beforeUpdate(ctx, ent); err != nil {
		return fmt.Errorf("before update, %w", err)
	}

	if err := pus.execContext(ctx, ent); err != nil {
		if isConflictError(err, pus.dbDriver) {
			return ErrConflict
		}
		return err
	}

	if v, ok := ent.(Cacheable); ok {
		if err := DeleteCache(ctx, v); err != nil {
			return fmt.Errorf("delete cache, %w", err)
		}
	}

	if err := afterUpdate(ctx, ent); err != nil {
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

func beforeInsert(ctx context.Context, ent Entity) error {
	if v, ok := ent.(BeforeInsertHook); ok {
		return v.BeforeInsert(ctx)
	} else if v, ok := ent.(EventHook); ok {
		return v.OnEntityEvent(ctx, EventBeforeInsert)
	}
	return nil
}

func afterInsert(ctx context.Context, ent Entity) error {
	if v, ok := ent.(AfterInsertHook); ok {
		return v.AfterInsert(ctx)
	} else if v, ok := ent.(EventHook); ok {
		return v.OnEntityEvent(ctx, EventAfterInsert)
	}
	return nil
}

func beforeUpdate(ctx context.Context, ent Entity) error {
	if v, ok := ent.(BeforeUpdateHook); ok {
		return v.BeforeUpdate(ctx)
	} else if v, ok := ent.(EventHook); ok {
		return v.OnEntityEvent(ctx, EventBeforeUpdate)
	}
	return nil
}

func afterUpdate(ctx context.Context, ent Entity) error {
	if v, ok := ent.(AfterUpdateHook); ok {
		return v.AfterUpdate(ctx)
	} else if v, ok := ent.(EventHook); ok {
		return v.OnEntityEvent(ctx, EventAfterUpdate)
	}
	return nil
}

func beforeDelete(ctx context.Context, ent Entity) error {
	if v, ok := ent.(BeforeDeleteHook); ok {
		return v.BeforeDelete(ctx)
	} else if v, ok := ent.(EventHook); ok {
		return v.OnEntityEvent(ctx, EventBeforeDelete)
	}
	return nil
}

func afterDelete(ctx context.Context, ent Entity) error {
	if v, ok := ent.(AfterDeleteHook); ok {
		return v.AfterDelete(ctx)
	} else if v, ok := ent.(EventHook); ok {
		return v.OnEntityEvent(ctx, EventAfterDelete)
	}
	return nil
}
