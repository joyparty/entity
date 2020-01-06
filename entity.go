package entity

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/jmoiron/sqlx/reflectx"
	"github.com/pkg/errors"
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
		return nil, errors.Errorf("empty entity %q", md.Type)
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
		return nil, errors.Errorf("undefined entity %q primary key", md.Type)
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

func getColumns(ent Entity) []Column {
	sm := mapper.TypeMap(reflectx.Deref(reflect.TypeOf(ent)))

	cols := []Column{}
	for _, fi := range sm.Names {
		if fi.Parent.Path != "" {
			continue
		}

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

// Load 从数据库载入entity
func Load(ctx context.Context, ent Entity, db DB) error {
	ctx, cancel := context.WithTimeout(ctx, ReadTimeout)
	defer cancel()

	cv, cacheable := ent.(Cacheable)
	if cacheable {
		if loaded, err := loadCache(cv); err != nil {
			return errors.WithMessage(err, "load entity from cache")
		} else if loaded {
			return nil
		}
	}

	if err := doLoad(ctx, ent, db); err != nil {
		return errors.WithMessage(err, "load entity from db")
	}

	if cacheable {
		if err := SaveCache(cv); err != nil {
			return errors.WithMessage(err, "found entity")
		}
	}

	return nil
}

// Insert 插入新entity
func Insert(ctx context.Context, ent Entity, db DB) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := ent.OnEntityEvent(ctx, EventBeforeInsert); err != nil {
		return 0, errors.WithMessage(err, "before insert entity")
	}

	lastID, err := doInsert(ctx, ent, db)
	if err != nil {
		if isConflictError(db.DriverName(), err) {
			return 0, errors.Wrap(ErrConflict, "insert entity")
		}
		return 0, errors.WithMessage(err, "insert entity")
	}

	if err := ent.OnEntityEvent(ctx, EventAfterInsert); err != nil {
		return 0, errors.WithMessage(err, "after insert entity")
	}

	return lastID, nil
}

// Update 更新entity
func Update(ctx context.Context, ent Entity, db DB) error {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := ent.OnEntityEvent(ctx, EventBeforeUpdate); err != nil {
		return errors.WithMessage(err, "before update entity")
	}

	if err := doUpdate(ctx, ent, db); err != nil {
		return errors.WithMessage(err, "update entity")
	}

	if v, ok := ent.(Cacheable); ok {
		if err := DeleteCache(v); err != nil {
			return errors.WithMessage(err, "after update entity")
		}
	}

	return errors.WithMessage(
		ent.OnEntityEvent(ctx, EventAfterUpdate),
		"after update entity",
	)
}

// Delete 删除entity
func Delete(ctx context.Context, ent Entity, db DB) error {
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()

	if err := ent.OnEntityEvent(ctx, EventBeforeDelete); err != nil {
		return err
	}

	if err := doDelete(ctx, ent, db); err != nil {
		return err
	}

	if v, ok := ent.(Cacheable); ok {
		if err := DeleteCache(v); err != nil {
			return errors.WithMessage(err, "after delete entity")
		}
	}

	return errors.WithMessage(
		ent.OnEntityEvent(ctx, EventAfterDelete),
		"after delete entity",
	)
}
