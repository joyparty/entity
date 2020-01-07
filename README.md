## 介绍

基于sqlx库，封装了实体对象的基本CRUD方法，除数据库读写外，还实现了自定义缓存机制，在数据库读写过程中，自动使用和更新缓存

样例代码见[example.go](./example/example.go)内

## Struct Tag

``` golang
type User struct {
	ID       int64 `db:"user_id,primaryKey,autoIncrement"`
	CreateAt int64 `db:"create_at,refuseUpdate,returningInsert"`
	UpdateAt int64 `db:"update_at"`
	Other    bool  `db:"-"`
}
```

实体配置，写在`db`内

可用tag:

- `primaryKey` 主键字段，每个实体对象至少要声明一个。别名：`primary_key`
- `refuseUpdate` 不允许更新，UPDATE时会被忽略，当设置了`primaryKey`或`autoIncrement`或`returningUpdate`时，这个配置会自动生效。别名: `refuse_update`
- `autoIncrement` 自增长主键，构造INSERT时此字段会被忽略。别名: `auto_increment`
- `returningInsert` insert时，这个字段会被放到`RETURNING`子句内返回，无论使用的数据库是否支持`RETURNING`。别名: `returning_insert`
- `returningUpdate` update时，这个字段会被放到`RETURNING`子句内返回，无论使用的数据库是否支持`RETURNING`。别名: `returning_update`
- `returning` 等于同时使用`returningInsert`和`returningUpdate`
