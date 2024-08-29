package entity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/doug-martin/goqu/v9"
)

// Factory 实体工厂函数
type Factory[ID any, Ent Entity] func(ID) Ent

// Repository 实体仓库
type Repository[ID any, Ent Entity] struct {
	db      DB
	factory Factory[ID, Ent]
}

// NewRepository 创建实体仓库
func NewRepository[ID any, Ent Entity](db DB, factory Factory[ID, Ent]) *Repository[ID, Ent] {
	return &Repository[ID, Ent]{
		db:      db,
		factory: factory,
	}
}

// Find 根据主键查询实体
func (repo *Repository[ID, Ent]) Find(ctx context.Context, id ID) (Ent, error) {
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
func (repo *Repository[ID, Ent]) Create(ctx context.Context, ent Ent) error {
	_, err := Insert(ctx, ent, repo.db)
	return err
}

// Save 更新实体
func (repo *Repository[ID, Ent]) Save(ctx context.Context, ent Ent) error {
	return Update(ctx, ent, repo.db)
}

// Update 根据ID查询实体并执行更新函数，apply 返回 true 时保存实体，返回 false 时不保存
func (repo *Repository[ID, Ent]) Update(ctx context.Context, id ID, apply func(ent Ent) (bool, error)) error {
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
func (repo *Repository[ID, Ent]) Delete(ctx context.Context, ent Ent) error {
	return Delete(ctx, ent, repo.db)
}

// ForEach 根据查询遍历实体，apply 返回 true 时继续遍历，返回 false 时停止遍历
func (repo *Repository[ID, Ent]) ForEach(ctx context.Context, stmt *goqu.SelectDataset, iteratee func(ent Ent) (bool, error)) error {
	query, args, err := stmt.ToSQL()
	if err != nil {
		return fmt.Errorf("build sql, %w", err)
	}

	rows, err := repo.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var row Ent

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

// UpdateByQuery 查询并更新，apply 返回 true 时更新实体，返回 false 时不更新
func (repo *Repository[ID, Ent]) UpdateByQuery(ctx context.Context, stmt *goqu.SelectDataset, apply func(ent Ent) (bool, error)) error {
	return repo.ForEach(ctx, stmt, func(ent Ent) (bool, error) {
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
func (repo *Repository[ID, Ent]) PageQuery(ctx context.Context, stmt *goqu.SelectDataset, currentPage, pageSize int) ([]Ent, Pagination, error) {
	total, err := GetTotalCount(ctx, repo.db, stmt)
	if err != nil {
		return nil, Pagination{}, fmt.Errorf("query total count, %w", err)
	}

	result := []Ent{}
	page := NewPagination(currentPage, pageSize, total)
	if total == 0 {
		return result, page, nil
	}

	stmt = stmt.Limit(uint(page.Limit())).Offset(uint(page.Offset()))
	if err := GetRecords(ctx, &result, repo.db, stmt); err != nil {
		return nil, Pagination{}, err
	}
	return result, page, nil
}
