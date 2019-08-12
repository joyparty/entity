## 介绍

基于sqlx库，封装了实体对象的基本CRUD方法，除数据库读写外，还实现了自定义缓存机制，在数据库读写过程中，自动使用和更新缓存

样例代码见[example.go](./example/example.go)内

## Struct Tag

``` golang
type User struct {
	ID       int64 `db:"user_id" entity:"primaryKey,autoIncrement"`
	CreateAt int64 `db:"create_at" entity:"refuseUpdate"`
	UpdateAt int64 `db:"update_at"`
	Other    bool  `db:"other" entity:"deprecated"`
}
```

每个字段属性，必须声明`db`，否则不会作为字段对待

实体配置，写在`entity`内

可用tag:

- `primaryKey` 主键字段，每个实体对象至少要声明一个
- `refuseUpdate` 不允许更新，UPDATE时会被忽略
- `autoIncrement` 自增长主键，构造INSERT时此字段会被忽略
- `returning` 如果数据库支持`RETURNING`特性，构造INSERT和UPDATE是，此字段的结果会放在`RETURNING`子句内返回
- `deprecated` 标记为废弃，构造CRUD查询时会被忽略掉
