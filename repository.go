package entity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/doug-martin/goqu/v9"
)

// Row is an entity row interface.
type Row[ID comparable] interface {
	Entity

	SetID(ID) error
}

// Repository is a generic repository for entity objects.
type Repository[ID comparable, R Row[ID]] struct {
	db      DB
	rowType reflect.Type
	factory func(ID) (R, error)
}

// NewRepository creates a new Repository instance.
func NewRepository[ID comparable, R Row[ID]](db DB) *Repository[ID, R] {
	var row R
	rt := reflect.TypeOf(row)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}

	return &Repository[ID, R]{
		db:      db,
		rowType: rt,
		factory: func(id ID) (R, error) {
			row := reflect.New(rt).Interface().(R)
			if err := row.SetID(id); err != nil {
				return row, fmt.Errorf("set id, %w", err)
			}
			return row, nil
		},
	}
}

// GetDB returns the database connection used by the repository.
func (r *Repository[ID, R]) GetDB() DB {
	return r.db
}

// NewEntity creates a new entity object with the given ID.
func (r *Repository[ID, R]) NewEntity(id ID) (R, error) {
	return r.factory(id)
}

// Find retrieves an entity by its primary key.
func (r *Repository[ID, R]) Find(ctx context.Context, id ID) (R, error) {
	row, err := r.factory(id)
	if err != nil {
		return row, fmt.Errorf("new row, %w", err)
	}

	if err := Load(ctx, row, r.db); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return row, ErrNotFound
		}
		return row, err
	}
	return row, nil
}

// Create saves a new entity to the database.
func (r *Repository[ID, R]) Create(ctx context.Context, row R) error {
	_, err := Insert(ctx, row, r.db)
	return err
}

// Update updates an existing entity.
func (r *Repository[ID, R]) Update(ctx context.Context, row R) error {
	return Update(ctx, row, r.db)
}

// UpdateBy retrieves an entity by ID and executes the apply function to update it. If apply returns false, changes are not saved.
func (r *Repository[ID, R]) UpdateBy(ctx context.Context, id ID, apply func(row R) (bool, error)) error {
	row, err := r.Find(ctx, id)
	if err != nil {
		return err
	} else if ok, err := apply(row); err != nil {
		return err
	} else if ok {
		return r.Update(ctx, row)
	}
	return nil
}

// Upsert inserts a new entity or updates an existing one.
func (r *Repository[ID, R]) Upsert(ctx context.Context, row R) error {
	return Upsert(ctx, row, r.db)
}

// Delete removes an entity from the database.
func (r *Repository[ID, R]) Delete(ctx context.Context, row R) error {
	return Delete(ctx, row, r.db)
}

// ForEach iterates over entities matching the query statement. The iteratee function should return false to stop iteration.
func (r *Repository[ID, R]) ForEach(ctx context.Context, stmt *goqu.SelectDataset, iteratee func(row R) (bool, error)) error {
	query, args, err := stmt.ToSQL()
	if err != nil {
		return fmt.Errorf("build sql, %w", err)
	}

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		row := reflect.New(r.rowType).Interface().(R)

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

// UpdateByQuery queries for entities and updates them using the apply function. If apply returns false for a row, that update is skipped.
func (r *Repository[ID, R]) UpdateByQuery(ctx context.Context, stmt *goqu.SelectDataset, apply func(row R) (bool, error)) error {
	return r.ForEach(ctx, stmt, func(row R) (bool, error) {
		if ok, err := apply(row); err != nil || !ok {
			return false, err
		} else if err := r.Update(ctx, row); err != nil {
			return false, err
		}
		return true, nil
	})
}

// Get retrieves a single entity matching the query statement.
func (r *Repository[ID, R]) Get(ctx context.Context, stmt *goqu.SelectDataset) (R, error) {
	row := reflect.New(r.rowType).Interface().(R)
	if err := GetRecord(ctx, row, r.db, stmt); err != nil {
		var x R
		if errors.Is(err, sql.ErrNoRows) {
			return x, ErrNotFound
		}

		return x, err
	}

	return row, nil
}

// Query retrieves a list of entities matching the query statement.
func (r *Repository[ID, R]) Query(ctx context.Context, stmt *goqu.SelectDataset) ([]R, error) {
	var rows []R
	if err := GetRecords(ctx, &rows, r.db, stmt); err != nil {
		return nil, err
	}
	return rows, nil
}

// PageQuery retrieves a paginated list of entities matching the query statement.
func (r *Repository[ID, R]) PageQuery(ctx context.Context, stmt *goqu.SelectDataset, currentPage, pageSize int) (rows []R, page Pagination, err error) {
	total, err := GetTotalCount(ctx, r.db, stmt)
	if err != nil {
		err = fmt.Errorf("query total count, %w", err)
		return
	}

	page = NewPagination(currentPage, pageSize, total)
	if total == 0 {
		return
	}

	stmt = stmt.Limit(page.ULimit()).Offset(page.UOffset())
	err = GetRecords(ctx, &rows, r.db, stmt)
	return
}

// PersistentObject is an interface for domain objects that can be persisted to the database.
type PersistentObject[ID comparable, DO any] interface {
	Row[ID]

	GetID() ID
	Set(context.Context, DO) error
	ToDomainObject() (DO, error)
}

// DomainObjectRepository is a repository for domain objects.
//
// Note: Some methods accept *goqu.SelectDataset parameters, which exposes technical implementation details
// and violates DDD principles. Therefore, DomainObjectRepository should not be used as a final implementation
// but rather as a component of the final implementation.
type DomainObjectRepository[ID comparable, DO any, PO PersistentObject[ID, DO]] struct {
	poRepository *Repository[ID, PO]
	poType       reflect.Type
}

// NewDomainObjectRepository creates a new DomainObjectRepository.
func NewDomainObjectRepository[ID comparable, DO any, PO PersistentObject[ID, DO]](
	persistentRepository *Repository[ID, PO],
) *DomainObjectRepository[ID, DO, PO] {
	var x PO
	poType := reflect.TypeOf(x)
	if poType.Kind() == reflect.Pointer {
		poType = poType.Elem()
	}

	return &DomainObjectRepository[ID, DO, PO]{
		poRepository: persistentRepository,
		poType:       poType,
	}
}

// Find retrieves a domain object by ID.
func (r *DomainObjectRepository[ID, DO, PO]) Find(ctx context.Context, id ID) (DO, error) {
	po, err := r.poRepository.Find(ctx, id)
	if err != nil {
		var x DO
		return x, err
	}

	return po.ToDomainObject()
}

// Create saves a new domain object to the database.
func (r *DomainObjectRepository[ID, DO, PO]) Create(ctx context.Context, do DO) error {
	po, err := r.NewPersistentObject(ctx, do)
	if err != nil {
		return fmt.Errorf("new persistent object, %w", err)
	}

	return r.poRepository.Create(ctx, po)
}

// Update updates an existing domain object in the database.
func (r *DomainObjectRepository[ID, DO, PO]) Update(ctx context.Context, do DO) error {
	po, err := r.NewPersistentObject(ctx, do)
	if err != nil {
		return fmt.Errorf("new persistent object, %w", err)
	}

	return r.poRepository.Update(ctx, po)
}

// UpdateBy retrieves a domain object by ID and updates it using the apply function.
func (r *DomainObjectRepository[ID, DO, PO]) UpdateBy(ctx context.Context, id ID, apply func(do DO) (bool, error)) error {
	return r.poRepository.UpdateBy(ctx, id, func(po PO) (ok bool, err error) {
		defer func() {
			if err != nil {
				err = fmt.Errorf("id %v, %w", id, err)
			}
		}()

		if v, err := po.ToDomainObject(); err != nil {
			return false, fmt.Errorf("convert to domain object, %w", err)
		} else if ok, err := apply(v); err != nil || !ok {
			return false, err
		} else if err := po.Set(ctx, v); err != nil {
			return false, fmt.Errorf("set persistent object, %w", err)
		}

		return true, nil
	})
}

// UpdateByQuery queries for domain objects and updates them using the apply function.
func (r *DomainObjectRepository[ID, DO, PO]) UpdateByQuery(ctx context.Context, stmt *goqu.SelectDataset, apply func(do DO) (bool, error)) error {
	return r.poRepository.UpdateByQuery(ctx, stmt, func(po PO) (ok bool, err error) {
		defer func() {
			if err != nil {
				err = fmt.Errorf("id %v, %w", po.GetID(), err)
			}
		}()

		if v, err := po.ToDomainObject(); err != nil {
			return false, fmt.Errorf("convert to domain object, %w", err)
		} else if ok, err := apply(v); err != nil || !ok {
			return false, err
		} else if err := po.Set(ctx, v); err != nil {
			return false, fmt.Errorf("set persistent object, %w", err)
		}

		return true, nil
	})
}

// Upsert inserts a new domain object or updates an existing one.
func (r *DomainObjectRepository[ID, DO, PO]) Upsert(ctx context.Context, do DO) error {
	po, err := r.NewPersistentObject(ctx, do)
	if err != nil {
		return fmt.Errorf("new persistent object, %w", err)
	}

	return r.poRepository.Upsert(ctx, po)
}

// Delete removes a domain object from the database.
func (r *DomainObjectRepository[ID, DO, PO]) Delete(ctx context.Context, do DO) error {
	po, err := r.NewPersistentObject(ctx, do)
	if err != nil {
		return fmt.Errorf("new persistent object, %w", err)
	}

	return r.poRepository.Delete(ctx, po)
}

// ForEach iterates over domain objects matching the query. The iteratee function should return false to stop iteration.
func (r *DomainObjectRepository[ID, DO, PO]) ForEach(ctx context.Context, stmt *goqu.SelectDataset, iteratee func(do DO) (bool, error)) error {
	return r.poRepository.ForEach(ctx, stmt, func(po PO) (ok bool, err error) {
		defer func() {
			if err != nil {
				err = fmt.Errorf("id %v, %w", po.GetID(), err)
			}
		}()

		if do, err := po.ToDomainObject(); err != nil {
			return false, fmt.Errorf("convert to domain object, %w", err)
		} else if ok, err := iteratee(do); err != nil || !ok {
			return false, err
		}

		return true, nil
	})
}

// Get retrieves a single domain object matching the query statement.
func (r *DomainObjectRepository[ID, DO, PO]) Get(ctx context.Context, stmt *goqu.SelectDataset) (DO, error) {
	po, err := r.poRepository.Get(ctx, stmt)
	if err != nil {
		var x DO
		return x, err
	}

	return po.ToDomainObject()
}

// Query retrieves a list of domain objects matching the query statement.
func (r *DomainObjectRepository[ID, DO, PO]) Query(ctx context.Context, stmt *goqu.SelectDataset) ([]DO, error) {
	rows, err := r.poRepository.Query(ctx, stmt)
	if err != nil {
		return nil, err
	}

	return r.ToDomainObjects(rows)
}

// PageQuery retrieves a paginated list of domain objects matching the query statement.
func (r *DomainObjectRepository[ID, DO, PO]) PageQuery(ctx context.Context, stmt *goqu.SelectDataset, currentPage, pageSize int) ([]DO, Pagination, error) {
	rows, page, err := r.poRepository.PageQuery(ctx, stmt, currentPage, pageSize)
	if err != nil {
		return nil, Pagination{}, err
	}

	items, err := r.ToDomainObjects(rows)
	if err != nil {
		return nil, Pagination{}, fmt.Errorf("convert to domain objects, %w", err)
	}

	return items, page, nil
}

// ToDomainObjects converts a slice of persistent objects to domain objects.
func (r *DomainObjectRepository[ID, DO, PO]) ToDomainObjects(src []PO) ([]DO, error) {
	result := make([]DO, 0, len(src))
	for _, po := range src {
		do, err := po.ToDomainObject()
		if err != nil {
			return nil, fmt.Errorf("id %v, %w", po.GetID(), err)
		}
		result = append(result, do)
	}

	return result, nil
}

// NewPersistentObject creates a new persistent object from a domain object.
func (r *DomainObjectRepository[ID, DO, PO]) NewPersistentObject(ctx context.Context, do DO) (PO, error) {
	po := reflect.New(r.poType).Interface().(PO)
	return po, po.Set(ctx, do)
}
