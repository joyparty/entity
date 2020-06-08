package entity

import (
	"context"
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
	ErrConflict = fmt.Errorf("database record conflict")

	// ReadTimeout 读取entity数据的默认超时时间
	ReadTimeout = 3 * time.Second
	// WriteTimeout 写入entity数据的默认超时时间
	WriteTimeout = 3 * time.Second

	entities    = map[reflect.Type]*Metadata{}
	entitiesMux sync.RWMutex

	mapper = reflectx.NewMapper("db")
)

// Event 存储事件
type Event int

// Entity 实体对象接口
type Entity interface {
	TableName() string
	OnEntityEvent(ctx context.Context, ev Event) error
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
		return nil, fmt.Errorf("undefined entity %q primary key", md.Type)
	}

	return md, nil
}

func getMetadata(ent Entity) (*Metadata, error) {
	t := reflectx.Deref(reflect.TypeOf(ent))

	entitiesMux.RLock()
	md, ok := entities[t]
	entitiesMux.RUnlock()
	if ok {
		return md, nil
	}

	entitiesMux.Lock()
	defer entitiesMux.Unlock()

	md, err := NewMetadata(ent)
	if err != nil {
		return nil, err
	}

	entities[t] = md
	return md, nil
}

func getColumns(ent Entity) []Column { // revive:disable-line
	sm := mapper.TypeMap(reflectx.Deref(reflect.TypeOf(ent)))

	cols := []Column{}
	for _, fi := range getFields(sm.Tree) {
		col := Column{
			StructField: fi.Field.Name,
			DBField:     fi.Name,
		}

		for key := range fi.Options {
			if key == "primaryKey" || key == "primary_key" {
				col.PrimaryKey = true
				col.RefuseUpdate = true
			} else if key == "refuseUpdate" || key == "refuse_update" {
				col.RefuseUpdate = true
			} else if key == "returning" {
				col.ReturningInsert = true
				col.ReturningUpdate = true
				col.RefuseUpdate = true
			} else if key == "returningInsert" || key == "returning_insert" {
				col.ReturningInsert = true
			} else if key == "returningUpdate" || key == "returning_update" {
				col.ReturningUpdate = true
				col.RefuseUpdate = true
			} else if key == "autoIncrement" || key == "auto_increment" {
				col.AutoIncrement = true
				col.RefuseUpdate = true
			}
		}
		cols = append(cols, col)
	}

	return cols
}

// 从反射信息内，解析字段属性
func getFields(node *reflectx.FieldInfo) []*reflectx.FieldInfo {
	fields := []*reflectx.FieldInfo{}
	for _, fi := range node.Children {
		if fi == nil {
			continue
		}

		if fi.Embedded {
			fields = append(fields, getFields(fi)...)
		} else {
			fields = append(fields, fi)
		}
	}

	// root node
	if node.Parent == nil {
		// replace duplicate name
		filter := map[string]*reflectx.FieldInfo{}
		for _, v := range fields {
			filter[v.Name] = v
		}

		fields = fields[:0]
		for _, v := range filter {
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
		if loaded, err := loadCache(cv); err != nil {
			return fmt.Errorf("load from cache, %w", err)
		} else if loaded {
			return nil
		}
	}

	if err := doLoad(ctx, ent, db); err != nil {
		return fmt.Errorf("load from database, %w", err)
	}

	if cacheable {
		if err := SaveCache(cv); err != nil {
			return fmt.Errorf("save cache, %w", err)
		}
	}

	return nil
}

// Insert 插入新entity
func Insert(ctx context.Context, ent Entity, db DB) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := ent.OnEntityEvent(ctx, EventBeforeInsert); err != nil {
		return 0, fmt.Errorf("before insert, %w", err)
	}

	lastID, err := doInsert(ctx, ent, db)
	if err != nil {
		if isConflictError(db, err) {
			return 0, ErrConflict
		}
		return 0, err
	}

	if err := ent.OnEntityEvent(ctx, EventAfterInsert); err != nil {
		return 0, fmt.Errorf("after insert, %w", err)
	}

	return lastID, nil
}

// Update 更新entity
func Update(ctx context.Context, ent Entity, db DB) error {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := ent.OnEntityEvent(ctx, EventBeforeUpdate); err != nil {
		return fmt.Errorf("before update, %w", err)
	}

	if err := doUpdate(ctx, ent, db); err != nil {
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

// Delete 删除entity
func Delete(ctx context.Context, ent Entity, db DB) error {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := ent.OnEntityEvent(ctx, EventBeforeDelete); err != nil {
		return fmt.Errorf("before delete, %w", err)
	}

	if err := doDelete(ctx, ent, db); err != nil {
		return err
	}

	if v, ok := ent.(Cacheable); ok {
		if err := DeleteCache(v); err != nil {
			return fmt.Errorf("delete cache, %w", err)
		}
	}

	if err := ent.OnEntityEvent(ctx, EventAfterDelete); err != nil {
		return fmt.Errorf("after delete, %w", err)
	}
	return nil
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
				err = fmt.Errorf("commit transaction, %w", err)
			}
		} else {
			_ = tx.Rollback()
		}
	}()

	return fn(tx)
}
