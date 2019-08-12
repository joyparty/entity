package entity

import (
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
	Tablename() string
	OnEntityEvent(ev Event) error
}

// Column 字段信息
type Column struct {
	StructField   string
	DBField       string
	PrimaryKey    bool
	AutoIncrement bool
	RefuseUpdate  bool
	Returning     bool
}

// Metadata 元数据
type Metadata struct {
	ID          string
	Tablename   string
	Columns     []Column
	PrimaryKeys []Column
}

// NewMetadata 构造实体对象元数据
func NewMetadata(entity Entity) (*Metadata, error) {
	id := entityID(entity)
	md := &Metadata{
		ID:          id,
		Tablename:   entity.Tablename(),
		Columns:     getColumns(entity),
		PrimaryKeys: []Column{},
	}

	if len(md.Columns) == 0 {
		return nil, errors.Errorf("empty entity %q", id)
	}

	for _, col := range md.Columns {
		if col.PrimaryKey {
			md.PrimaryKeys = append(md.PrimaryKeys, col)
		}
	}

	if len(md.PrimaryKeys) == 0 {
		return nil, errors.Errorf("undefined entity %q primary key", id)
	}

	return md, nil
}

func getMetadata(entity Entity) (*Metadata, error) {
	id := entityID(entity)
	if md, ok := entites[id]; ok {
		return md, nil
	}

	md, err := NewMetadata(entity)
	if err != nil {
		return nil, err
	}

	entites[id] = md
	return md, nil
}

func getColumns(entity Entity) []Column {
	cols := []Column{}

	v := reflect.TypeOf(entity).Elem()
	for i, len := 0, v.NumField(); i < len; i++ {
		field := v.Field(i)
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
				col.Returning = true
			} else if val == "autoIncrement" {
				col.AutoIncrement = true
			} else if val == "deprecated" {
				deprecated = true
			}
		}

		if !deprecated {
			cols = append(cols, col)
		}
	}
	return cols
}

func entityID(entity Entity) string {
	v := reflect.TypeOf(entity).Elem()
	return fmt.Sprintf("%s.%s", v.PkgPath(), v.Name())
}
