package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joyparty/entity"
)

var (
	defaultDB *sqlx.DB

	_ entity.Cacher = &fakeCacher{}

	// User is a cacheable assertion.
	_ entity.Cacheable = (*User)(nil)
	// User is an entity object assertion.
	_ entity.Entity = (*User)(nil)
)

// func init() {
// 	// Set the entity module read/write timeout (default is 3 seconds).
// 	entity.ReadTimeout = 5 * time.Second
// 	entity.WriteTimeout = 5 * time.Second
// }

func main() {
}

// User is a user entity.
type User struct {
	ID       int64 `db:"user_id,primaryKey,autoIncrement"`
	CreateAt int64 `db:"create_at,refuseUpdate"`
	UpdateAt int64 `db:"update_at"`
	Other    bool  `db:"-"`
}

// TableName returns the database table name and implements the entity.Entity interface.
func (u User) TableName() string {
	return "users"
}

// OnEntityEvent handles storage event callbacks and implements the entity.Entity interface.
func (u *User) OnEntityEvent(ctx context.Context, ev entity.Event) error {
	switch ev {
	case entity.EventBeforeInsert:
		u.CreateAt = time.Now().Unix()
	case entity.EventBeforeUpdate:
		u.UpdateAt = time.Now().Unix()
	}

	return nil
}

// CacheOption returns cache configuration and implements the entity.Cacheable interface.
// Without implementing this method, automatic caching will not be enabled.
func (u *User) CacheOption() entity.CacheOption {
	return entity.CacheOption{
		Key:        fmt.Sprintf(`user:entity:%d`, u.ID),
		Expiration: 10 * time.Minute,
		Cacher:     &fakeCacher{},
	}
}

// FindUser retrieves a user by ID.
func FindUser(ctx context.Context, id int64) (*User, error) {
	u := &User{ID: id}
	if err := entity.Load(ctx, u, defaultDB); err != nil {
		return nil, err
	}
	return u, nil
}

// InsertUser saves a new user to the database.
func InsertUser(ctx context.Context, u *User) error {
	return insertUser(ctx, u, defaultDB)
}

// InsertUserTx saves a new user using a database transaction.
func InsertUserTx(ctx context.Context, u *User, tx *sqlx.Tx) error {
	return insertUser(ctx, u, tx)
}

func insertUser(ctx context.Context, u *User, db entity.DB) error {
	id, err := entity.Insert(ctx, u, db)
	if err != nil {
		return err
	}

	u.ID = id
	return nil
}

// UpdateUser updates an existing user in the database.
func UpdateUser(ctx context.Context, u *User) error {
	return entity.Update(ctx, u, defaultDB)
}

// UpdateUserTx updates an existing user using a database transaction.
func UpdateUserTx(ctx context.Context, u *User, tx *sqlx.Tx) error {
	return entity.Update(ctx, u, tx)
}

// DeleteUser deletes a user from the database.
func DeleteUser(ctx context.Context, u *User) error {
	return entity.Delete(ctx, u, defaultDB)
}

// DeleteUserTx deletes a user using a database transaction.
func DeleteUserTx(ctx context.Context, u *User, tx *sqlx.Tx) error {
	return entity.Delete(ctx, u, tx)
}

type fakeCacher struct{}

func (fc *fakeCacher) Get(_ context.Context, key string) ([]byte, error) {
	return []byte{}, nil
}

func (fc *fakeCacher) Put(_ context.Context, key string, data []byte, expiration time.Duration) error {
	return nil
}

func (fc *fakeCacher) Delete(_ context.Context, key string) error {
	return nil
}
