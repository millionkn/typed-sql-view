# 为什么要写这个包

我不太喜欢写sql,像`typeORM`提供的`Repository`虽然简单的select能提供一些ts类型支持,但
- join是封装的不够好,没有join时依然能够被ts提示(尽管实际为undefined),
- groupBy需要sqlBuilder,没有类型提示,和直接写sql其实没有太大区别

`prisma`没用过,但看过文档应该也是走的类似`Repository`的路线,简单的select操作类型支持比`typeORM`好一些,但也没解决groupBy和复杂的join

# 基本使用
