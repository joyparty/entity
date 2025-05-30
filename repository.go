package entity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/doug-martin/goqu/v9"
)

// Row 实体行接口
type Row[ID comparable] interface {
	Entity

	SetID(ID) error
}

// Repository 实体仓库
type Repository[ID comparable, R Row[ID]] struct {
	db      DB
	factory func(ID) (R, error)
}

// NewRepository 创建实体仓库
func NewRepository[ID comparable, R Row[ID]](db DB) *Repository[ID, R] {
	var row R
	rt := reflect.TypeOf(row)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}

	return &Repository[ID, R]{
		db: db,
		factory: func(id ID) (R, error) {
			row := reflect.New(rt).Interface().(R)
			if err := row.SetID(id); err != nil {
				return row, fmt.Errorf("set id, %w", err)
			}
			return row, nil
		},
	}
}

// GetDB 获取数据库连接
func (repo *Repository[ID, R]) GetDB() DB {
	return repo.db
}

// NewEntity 创建实体对象
func (repo *Repository[ID, R]) NewEntity(id ID) (R, error) {
	return repo.factory(id)
}

// Find 根据主键查询实体
func (repo *Repository[ID, R]) Find(ctx context.Context, id ID) (R, error) {
	row, err := repo.factory(id)
	if err != nil {
		return row, fmt.Errorf("new row, %w", err)
	}

	if err := Load(ctx, row, repo.db); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return row, ErrNotFound
		}
		return row, err
	}
	return row, nil
}

// Create 保存新的实体
func (repo *Repository[ID, R]) Create(ctx context.Context, row R) error {
	_, err := Insert(ctx, row, repo.db)
	return err
}

// Save 更新实体
func (repo *Repository[ID, R]) Save(ctx context.Context, row R) error {
	return Update(ctx, row, repo.db)
}

// Update 根据ID查询实体并执行更新函数，apply return false则不保存
func (repo *Repository[ID, R]) Update(ctx context.Context, id ID, apply func(row R) (bool, error)) error {
	row, err := repo.Find(ctx, id)
	if err != nil {
		return err
	} else if ok, err := apply(row); err != nil {
		return err
	} else if ok {
		return repo.Save(ctx, row)
	}
	return nil
}

// Delete 删除实体
func (repo *Repository[ID, R]) Delete(ctx context.Context, row R) error {
	return Delete(ctx, row, repo.db)
}

// ForEach 根据查询遍历实体，iteratee return false则停止遍历
func (repo *Repository[ID, R]) ForEach(ctx context.Context, stmt *goqu.SelectDataset, iteratee func(row R) (bool, error)) error {
	query, args, err := stmt.ToSQL()
	if err != nil {
		return fmt.Errorf("build sql, %w", err)
	}

	rows, err := repo.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var v R
	vt := reflect.TypeOf(v)
	if vt.Kind() == reflect.Ptr {
		vt = vt.Elem()
	}

	for rows.Next() {
		row := reflect.New(vt).Interface().(R)

		if err := rows.StructScan(row); err != nil {
			return fmt.Errorf("scan row, %w", err)
		} else if ok, err := iteratee(row); err != nil {
			return err
		} else if !ok {
			break
		}
	}

	return rows.Err()
}

// UpdateByQuery 查询并更新，apply return false则放弃那一条的更新
func (repo *Repository[ID, R]) UpdateByQuery(ctx context.Context, stmt *goqu.SelectDataset, apply func(row R) (bool, error)) error {
	return repo.ForEach(ctx, stmt, func(row R) (bool, error) {
		if ok, err := apply(row); err != nil {
			return false, err
		} else if ok {
			if err := repo.Save(ctx, row); err != nil {
				return false, err
			}
		}

		return true, nil
	})
}

// Query 通过查询条件获取实体列表
func (repo *Repository[ID, R]) Query(ctx context.Context, stmt *goqu.SelectDataset) ([]R, error) {
	var rows []R
	if err := GetRecords(ctx, &rows, repo.db, stmt); err != nil {
		return nil, err
	}
	return rows, nil
}

// PageQuery 分页查询
func (repo *Repository[ID, R]) PageQuery(ctx context.Context, stmt *goqu.SelectDataset, currentPage, pageSize int) (rows []R, page Pagination, err error) {
	total, err := GetTotalCount(ctx, repo.db, stmt)
	if err != nil {
		err = fmt.Errorf("query total count, %w", err)
		return
	}

	page = NewPagination(currentPage, pageSize, total)
	if total == 0 {
		return
	}

	stmt = stmt.Limit(page.ULimit()).Offset(page.UOffset())
	err = GetRecords(ctx, &rows, repo.db, stmt)
	return
}
