package entity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/doug-martin/goqu/v9"
)

// Factory 实体工厂函数
type Factory[ID comparable, E Entity] func(ID) E

// Repository 实体仓库
type Repository[ID comparable, E Entity] struct {
	db      DB
	factory Factory[ID, E]
}

// NewRepository 创建实体仓库
func NewRepository[ID comparable, E Entity](db DB, factory Factory[ID, E]) *Repository[ID, E] {
	return &Repository[ID, E]{
		db:      db,
		factory: factory,
	}
}

// NewEntity 创建实体对象
func (repo *Repository[ID, E]) NewEntity(id ID) E {
	return repo.factory(id)
}

// Find 根据主键查询实体
func (repo *Repository[ID, E]) Find(ctx context.Context, id ID) (E, error) {
	ent := repo.factory(id)
	if err := Load(ctx, ent, repo.db); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ent, ErrNotFound
		}
		return ent, err
	}
	return ent, nil
}

// Create 保存新的实体
func (repo *Repository[ID, E]) Create(ctx context.Context, ent E) error {
	_, err := Insert(ctx, ent, repo.db)
	return err
}

// Save 更新实体
func (repo *Repository[ID, E]) Save(ctx context.Context, ent E) error {
	return Update(ctx, ent, repo.db)
}

// Update 根据ID查询实体并执行更新函数，apply return false则不保存
func (repo *Repository[ID, E]) Update(ctx context.Context, id ID, apply func(ent E) (bool, error)) error {
	ent, err := repo.Find(ctx, id)
	if err != nil {
		return err
	} else if ok, err := apply(ent); err != nil {
		return err
	} else if ok {
		return repo.Save(ctx, ent)
	}
	return nil
}

// Delete 删除实体
func (repo *Repository[ID, E]) Delete(ctx context.Context, ent E) error {
	return Delete(ctx, ent, repo.db)
}

// ForEach 根据查询遍历实体，iteratee return false则停止遍历
func (repo *Repository[ID, E]) ForEach(ctx context.Context, stmt *goqu.SelectDataset, iteratee func(ent E) (bool, error)) error {
	query, args, err := stmt.ToSQL()
	if err != nil {
		return fmt.Errorf("build sql, %w", err)
	}

	rows, err := repo.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	var v E
	vt := reflect.TypeOf(v)
	if vt.Kind() == reflect.Ptr {
		vt = vt.Elem()
	}

	for rows.Next() {
		row := reflect.New(vt).Interface().(E)

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
func (repo *Repository[ID, E]) UpdateByQuery(ctx context.Context, stmt *goqu.SelectDataset, apply func(ent E) (bool, error)) error {
	return repo.ForEach(ctx, stmt, func(ent E) (bool, error) {
		if ok, err := apply(ent); err != nil {
			return false, err
		} else if ok {
			if err := repo.Save(ctx, ent); err != nil {
				return false, err
			}
		}

		return true, nil
	})
}

// PageQuery 分页查询
func (repo *Repository[ID, E]) PageQuery(ctx context.Context, stmt *goqu.SelectDataset, currentPage, pageSize int) (items []E, page Pagination, err error) {
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
	err = GetRecords(ctx, &items, repo.db, stmt)
	return
}
