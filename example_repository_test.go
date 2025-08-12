package entity_test

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/joyparty/entity"
	"github.com/joyparty/entity/cache"
)

func init() {
	// Initialize the default cacher for entity package.
	entity.DefaultCacher = cache.NewMemoryCache()
}

// Account represents a user account.
//
// Put it in the domain layer.
type Account struct {
	ID        int
	Name      string
	Password  string
	CreatedAt int64
	UpdatedAt int64
}

// AccountFilter represents filters for querying accounts.
//
// Put it in the domain layer.
type AccountFilter struct {
	Keyword  string
	Page     int
	PageSize int
}

// AccountRepository defines the interface for account repository operations.
//
// Put it in the domain layer.
type AccountRepository interface {
	Find(ctx context.Context, id int) (*Account, error)
	Create(ctx context.Context, a *Account) error
	Update(ctx context.Context, a *Account) error
	UpdateBy(ctx context.Context, id int, apply func(a *Account) (bool, error)) error
	UpdateByFilter(ctx context.Context, filter AccountFilter, apply func(a *Account) (bool, error)) error
	Query(ctx context.Context, filter AccountFilter) ([]*Account, error)
	PageQuery(ctx context.Context, filter AccountFilter) ([]*Account, entity.Pagination, error)
}

// Put it in the infrastructure layer.
type accountRepository struct {
	db   entity.DB
	base *entity.DomainObjectRepository[int, *Account, *accountRow]
}

// Put it in the infrastructure layer.
func NewAccountRepository(db entity.DB) AccountRepository {
	return &accountRepository{
		db: db,
		base: entity.NewDomainObjectRepository(
			entity.NewRepository[int, *accountRow](db),
		),
	}
}

func (r *accountRepository) Find(ctx context.Context, id int) (*Account, error) {
	return r.base.Find(ctx, id)
}

func (r *accountRepository) FindByName(ctx context.Context, name string) (*Account, error) {
	stmt := selectAccounts.Where(colName.Eq(name)).Limit(1)

	return r.base.Get(ctx, stmt)
}

func (r *accountRepository) Create(ctx context.Context, a *Account) error {
	return r.base.Create(ctx, a)
}

func (r *accountRepository) Update(ctx context.Context, a *Account) error {
	return r.base.Update(ctx, a)
}

func (r *accountRepository) UpdateBy(ctx context.Context, id int, apply func(a *Account) (bool, error)) error {
	return r.base.UpdateBy(ctx, id, apply)
}

func (r *accountRepository) UpdateByFilter(ctx context.Context, filter AccountFilter, apply func(a *Account) (bool, error)) error {
	stmt := accountFilter(filter).ToSelect()
	return r.base.UpdateByQuery(ctx, stmt, apply)
}

func (r *accountRepository) Query(ctx context.Context, filter AccountFilter) ([]*Account, error) {
	stmt := accountFilter(filter).ToSelect().Order(colID.Desc())

	return r.base.Query(ctx, stmt)
}

func (r *accountRepository) PageQuery(ctx context.Context, filter AccountFilter) ([]*Account, entity.Pagination, error) {
	stmt := accountFilter(filter).ToSelect().Order(colID.Desc())

	return r.base.PageQuery(ctx, stmt, filter.Page, filter.PageSize)
}

// accountRow represents the database row for Account entity.
//
// Put it in the infrastructure layer.
type accountRow struct {
	ID        int          `db:"id,primaryKey,autoIncrement"`
	Name      string       `db:"name"`
	Password  string       `db:"password"`
	CreatedAt time.Time    `db:"created_at,refuseUpdate"`
	UpdatedAt sql.NullTime `db:"updated_at"`
}

// TableName implements entity.Entity interface.
func (row *accountRow) TableName() string {
	return "accounts"
}

func (row *accountRow) BeforeInsert(_ context.Context) error {
	row.CreatedAt = time.Now()
	return nil
}

func (row *accountRow) BeforeUpdate(_ context.Context) error {
	row.UpdatedAt = sql.NullTime{Time: time.Now(), Valid: true}
	return nil
}

// CacheOption implements entity.Cacheable interface.
//
// It defines how the entity should be cached.
func (row *accountRow) CacheOption() entity.CacheOption {
	return entity.CacheOption{
		Key:        fmt.Sprintf("account:%d", row.ID),
		Expiration: 5 * time.Minute,
	}
}

// SetID implements entity.Row interface.
func (row *accountRow) SetID(id int) error {
	row.ID = id
	return nil
}

// GetID implements entity.PersistentObject interface.
func (row *accountRow) GetID() int {
	return row.ID
}

// Set implements entity.PersistentObject interface.
func (row *accountRow) Set(_ context.Context, a *Account) error {
	row.ID = a.ID
	row.Name = a.Name
	row.Password = a.Password
	return nil
}

// ToDomainObject implements entity.PersistentObject interface.
func (row *accountRow) ToDomainObject() (*Account, error) {
	a := &Account{
		ID:        row.ID,
		Name:      row.Name,
		Password:  row.Password,
		CreatedAt: row.CreatedAt.Unix(),
	}

	if row.UpdatedAt.Valid {
		a.UpdatedAt = row.UpdatedAt.Time.Unix()
	}
	return a, nil
}

var (
	tableAccounts = goqu.T("accounts")

	selectAccounts = goqu.From(tableAccounts).Prepared(true)

	colID   = goqu.C("id")
	colName = goqu.C("name")
)

// Put it in the infrastructure layer.
type accountFilter AccountFilter

func (f accountFilter) Where() exp.Expression {
	conds := exp.NewExpressionList(exp.AndType)

	if f.Keyword != "" {
		conds = conds.Append(colName.RegexpLike(f.Keyword))
	}

	return conds
}

func (f accountFilter) ToSelect() *goqu.SelectDataset {
	return selectAccounts.Where(f.Where())
}
