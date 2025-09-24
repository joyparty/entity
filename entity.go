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
	// EventUnknown 未定义事件
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
	// ErrConflict 发生了数据冲突
	ErrConflict = errors.New("record conflict")

	// ErrNotFound 记录未找到错误
	ErrNotFound = errors.New("record not found")

	// ReadTimeout 读取entity数据的默认超时时间
	ReadTimeout = 3 * time.Second
	// WriteTimeout 写入entity数据的默认超时时间
	WriteTimeout = 3 * time.Second

	entities = &sync.Map{}

	mapper = reflectx.NewMapper("db")
)

// Event 存储事件
type Event int

// Entity 实体对象接口
type Entity interface {
	TableName() string
}

// EventHook 事件风格钩子
type EventHook interface {
	OnEntityEvent(ctx context.Context, ev Event) error
}

// BeforeInsertHook 在插入前调用
type BeforeInsertHook interface {
	BeforeInsert(ctx context.Context) error
}

// AfterInsertHook 在插入后调用
type AfterInsertHook interface {
	AfterInsert(ctx context.Context) error
}

// BeforeUpdateHook 在更新前调用
type BeforeUpdateHook interface {
	BeforeUpdate(ctx context.Context) error
}

// AfterUpdateHook 在更新后调用
type AfterUpdateHook interface {
	AfterUpdate(ctx context.Context) error
}

// BeforeDeleteHook 在删除前调用
type BeforeDeleteHook interface {
	BeforeDelete(ctx context.Context) error
}

// AfterDeleteHook 在删除后调用
type AfterDeleteHook interface {
	AfterDelete(ctx context.Context) error
}

// Column 字段信息
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

// Metadata 元数据
type Metadata struct {
	Type        reflect.Type
	TableName   string
	Columns     []Column
	PrimaryKeys []Column

	hasReturningInsert bool
	hasReturningUpdate bool
}

// NewMetadata 构造实体对象元数据
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

// 获取实体对象所有的db字段，支持嵌套结构体，外层字段优先级高于内层字段
func getFields(ent Entity) []*reflectx.FieldInfo {
	var get func(node *reflectx.FieldInfo) []*reflectx.FieldInfo

	get = func(node *reflectx.FieldInfo) []*reflectx.FieldInfo {
		fields := []*reflectx.FieldInfo{}

		var embedded []*reflectx.FieldInfo
		for _, v := range node.Children {
			if v != nil {
				if v.Embedded {
					embedded = append(embedded, v)
				} else {
					fields = append(fields, v)
				}
			}
		}

		for _, v := range embedded {
			fields = append(fields, get(v)...)
		}

		return fields
	}

	sm := mapper.TypeMap(reflectx.Deref(reflect.TypeOf(ent)))

	done := map[string]struct{}{}
	fields := []*reflectx.FieldInfo{}

	for _, v := range get(sm.Tree) {
		if _, ok := done[v.Name]; !ok {
			done[v.Name] = struct{}{}
			fields = append(fields, v)
		}
	}

	return fields
}

// Load 从数据库载入entity
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

// Insert 插入新entity
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

// Update 更新entity
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

// Upsert 插入或更新entity
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

// Delete 删除entity
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

// PrepareInsertStatement is a prepared insert statement for entity
type PrepareInsertStatement struct {
	md       *Metadata
	stmt     *sqlx.NamedStmt
	dbDriver string
}

// PrepareInsert returns a prepared insert statement for Entity
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

// ExecContext executes a prepared insert statement using the Entity passed.
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

// PrepareUpdate returns a prepared update statement for Entity
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

// ExecContext executes a prepared update statement using the Entity passed.
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
