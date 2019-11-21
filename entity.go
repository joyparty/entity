package entity

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

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

	entites = map[string]*Metadata{}
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
	ID          string
	TableName   string
	Columns     []Column
	PrimaryKeys []Column

	hasReturningInsert bool
	hasReturningUpdate bool
}

// NewMetadata 构造实体对象元数据
func NewMetadata(ent Entity) (*Metadata, error) {
	columns, err := getColumns(ent)
	if err != nil {
		return nil, errors.WithMessage(err, "entity metadata")
	}

	id := entityID(ent)
	md := &Metadata{
		ID:          id,
		TableName:   ent.TableName(),
		Columns:     columns,
		PrimaryKeys: []Column{},
	}

	if len(md.Columns) == 0 {
		return nil, errors.Errorf("empty entity %q", id)
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
		return nil, errors.Errorf("undefined entity %q primary key", id)
	}

	return md, nil
}

func getMetadata(ent Entity) (*Metadata, error) {
	id := entityID(ent)
	if md, ok := entites[id]; ok {
		return md, nil
	}

	md, err := NewMetadata(ent)
	if err != nil {
		return nil, err
	}

	entites[id] = md
	return md, nil
}

func getColumns(ent Entity) ([]Column, error) {
	cols := []Column{}

	rt := reflect.TypeOf(ent)
	if rt.Kind() != reflect.Ptr {
		return nil, errors.Errorf("entity columns, non-pointer %s", rt.String())
	}
	rt = rt.Elem()

	for i, len := 0, rt.NumField(); i < len; i++ {
		field := rt.Field(i)
		dbField := field.Tag.Get("db")
		if dbField == "" || dbField == "-" {
			continue
		}

		col := Column{
			StructField: field.Name,
			DBField:     dbField,
		}

		deprecated := false
		tags := strings.Split(field.Tag.Get("entity"), ",")
		for _, val := range tags {
			val = strings.TrimSpace(val)
			if val == "primaryKey" {
				col.PrimaryKey = true
				col.RefuseUpdate = true
			} else if val == "refuseUpdate" {
				col.RefuseUpdate = true
			} else if val == "returning" {
				col.ReturningInsert = true
				col.ReturningUpdate = true
				col.RefuseUpdate = true
			} else if val == "returningInsert" {
				col.ReturningInsert = true
			} else if val == "returningUpdate" {
				col.ReturningUpdate = true
				col.RefuseUpdate = true
			} else if val == "autoIncrement" {
				col.AutoIncrement = true
				col.RefuseUpdate = true
			} else if val == "deprecated" {
				deprecated = true
			}
		}

		if !deprecated {
			cols = append(cols, col)
		}
	}
	return cols, nil
}

func entityID(ent Entity) string {
	v := reflect.TypeOf(ent).Elem()
	return fmt.Sprintf("%s.%s", v.PkgPath(), v.Name())
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
