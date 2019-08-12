package example

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"gitlab.haochang.tv/gopkg/entity"
)

var (
	defaultDB *sqlx.DB

	_ entity.Cacher = &fakeCacher{}

	// User可缓存断言
	_ entity.Cacheable = (*User)(nil)
	// User是一个实体对象断言
	_ entity.Entity = (*User)(nil)
)

// func init() {
// 	// 设置entity模块读写超时时间，默认都是3秒
// 	entity.ReadTimeout = 5 * time.Second
// 	entity.WriteTimeout = 5 * time.Second
// }

// User 用户实体
type User struct {
	ID       int64 `db:"user_id" entity:"primaryKey,autoIncrement"`
	CreateAt int64 `db:"create_at" entity:"refuseUpdate"`
	UpdateAt int64 `db:"update_at"`
	Other    bool  `db:"other" entity:"deprecated"`
}

// Tablename 返回数据库表名，entity.Entity接口方法
func (u User) Tablename() string {
	return "users"
}

// OnEntityEvent 存储事件回调方法，entity.Entity接口方法
func (u *User) OnEntityEvent(ev entity.Event) error {
	switch ev {
	case entity.EventBeforeInsert:
		u.CreateAt = time.Now().Unix()
	case entity.EventBeforeUpdate:
		u.UpdateAt = time.Now().Unix()
	}

	return nil
}

// CacheOption 缓存配置，不实现这个方法就不会自动缓存，entity.Cacheable接口方法
func (u *User) CacheOption() entity.CacheOption {
	return entity.CacheOption{
		Key:        fmt.Sprintf(`user:entity:%d`, u.ID),
		Expiration: 10 * time.Minute,
		Cacher:     &fakeCacher{},
	}
}

// FindUser 根据ID查询用户
func FindUser(ctx context.Context, id int64) (*User, error) {
	u := &User{ID: id}
	if err := entity.Load(ctx, u, defaultDB); err != nil {
		return nil, err
	}
	return u, nil
}

// InsertUser 保存新用户
func InsertUser(ctx context.Context, u *User) error {
	return insertUser(ctx, u, defaultDB)
}

// InsertUserTx 使用事务保存新用户
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

// UpdateUser 更新用户数据
func UpdateUser(ctx context.Context, u *User) error {
	return entity.Update(ctx, u, defaultDB)
}

// UpdateUserTx 使用事务更新用户数据
func UpdateUserTx(ctx context.Context, u *User, tx *sqlx.Tx) error {
	return entity.Update(ctx, u, tx)
}

// DeleteUser 删除用户
func DeleteUser(ctx context.Context, u *User) error {
	return entity.Delete(ctx, u, defaultDB)
}

// DeleteUserTx 使用事务删除用户
func DeleteUserTx(ctx context.Context, u *User, tx *sqlx.Tx) error {
	return entity.Delete(ctx, u, tx)
}

type fakeCacher struct{}

func (fc *fakeCacher) Get(key string) ([]byte, error) {
	return []byte{}, nil
}

func (fc *fakeCacher) Put(key string, data []byte, expiration time.Duration) error {
	return nil
}

func (fc *fakeCacher) Delete(key string) error {
	return nil
}
