# 为什么要写这个包

像`typeORM`提供的`Repository`虽然简单的select能提供一些ts类型支持,但
+ join是封装的不够好,没有join时依然能够被ts提示(尽管实际为undefined),
+ groupBy需要sqlBuilder,没有类型提示,和直接写sql其实没有太大区别

`prisma`没用过,但看过文档应该也是走的类似`Repository`的路线,简单的select操作类型支持比`typeORM`好一些,但也没解决groupBy和复杂的join

相比`typeORM`的`builder`,主要用来向上层隐藏*列从哪来*以及*哪些行能被访问*，当然也包括更好一些的类型安全，最常见的就是根据用户仅能访问其所属公司的数据<br/>例：

```typescript
// 业务初期，没有考虑权限问题
type UserInfo = {
  comanyId:string,
  isAdmin:false
} | {
  comanyId:null,
  isAdmin:true
}

export const getView = (userInfo:UserInfo)=> createFromDefine(()=>{
  //...
})
// 后面要求除管理员外仅能访问自己公司的数据只需添加
// .andWhere(({ref,param})=>!userInfo.isAdmin && `${ref((e)=>e.companyId)} = ${param(userInfo.companyId)}`)

```

这样无论上层如何使用view(`groupBy`，`join`或其他操作等，当然也可以继续`andWhere`)，都是完全无感的

# 基本使用

## 例子
```typescript

import { RawSqlCreator, SqlExecutor, createFromDefine } from 'typed-sql-view'
import z from 'zod'//可以使用其他类型验证库或手动转换

const companyTableDefine = createFromDefine(`"public"."tableName1"`, (define) => {
  return {
    companyId: define((alias) => `"${alias}"."column_a"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
    companyType: define((alias) => `"${alias}"."column_b"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
    name: define((alias) => `"${alias}"."column_c"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
  }
})

const personTableDefine = createFromDefine(`"public"."tableName2"`, (define) => {
  const companyId = define((alias) => `"${alias}"."column_a"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw))
  const identify = define((alias) => `"${alias}"."column_b"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw))
  return {
    companyId,
    identify,
    primaryKeys: [companyId, identify],

    //可以延迟设置 'format' 与 'withNull'
    scoreValue: define((alias) => `"${alias}"."column_c"`),
    name: define((alias) => `"${alias}"."column_d"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
  }
})

const view = personTableDefine
  //根据业务要求,知道person最多有一个company,无论是否join 'company表'都不会影响'person表'的数量
  //所以可以使用lazy,如果后面没有用到,就不进行join
  //ps:left和inner都会立刻join,因为表的数量可能会被影响
  .join('lazy', true, companyTableDefine, ({ ref }) => `${ref((e) => e.base.companyId)} = ${ref((e) => e.extra.companyId)}`)
  .pipe((view) => {
    return view
      .groupBy((e) => e.extra.companyType, (define) => {
        return {
          //由于后面没有使用minScore,所以实际不会被选择
          minScore: define((ref) => `min(${ref((e) => e.base.scoreValue)})`),
          maxScore: define((ref) => `max(${ref((e) => e.base.scoreValue)})`),
        }
      })
      //这里由于groupBy了,所以会使用'having'而不是'where'
      .andWhere(({ ref, param }) => `${ref((e) => e.content.minScore)} > ${param(0)}`)
      //根据过滤条件，确定结果不会为null
      .mapTo((e) => {
        return {
          companyType: e.keys,
          ...e.content,
          minScore: e.content.minScore.withNull(false),
        }
      })
  })
  .pipe((aggrateView) => {
    return companyTableDefine
      // 由于声明了join的withNull为true
      // 所以minScore的withNull变回true,
      // maxScore的withNull会由boolean变为true,但查询结果的推断类型相同
      .join('lazy', true, aggrateView, ({ ref }) => `${ref((e) => e.base.companyType)} = ${ref((e) => e.extra.companyType)}`)
  })

const view = personTableDefine
  //根据业务要求,知道person最多有一个company,无论是否join 'company表'都不会影响'person表'的数量
  //所以可以使用lazy,如果后面没有用到,就不进行join
  //ps:left和inner都会立刻join,因为表的数量可能会被影响
  .join('lazy', true, companyTableDefine, ({ ref }) => `${ref((e) => e.base.companyId)} = ${ref((e) => e.extra.companyId)}`)
  .pipe((view) => {
    return view
      .groupBy((e) => e.extra.companyType, (define) => {
        return {
          //由于后面没有使用minScore,所以实际不会被选择
          minScore: define((ref) => `min(${ref((e) => e.base.scoreValue)})`),
          maxScore: define((ref) => `max(${ref((e) => e.base.scoreValue)})`),
        }
      })
      //这里由于groupBy了,所以会使用'having'而不是'where'
      .andWhere(({ ref, param }) => `${ref((e) => e.content.minScore)} > ${param(0)}`)
      //根据过滤条件，确定结果不会为null
      .mapTo((e) => {
        return {
          companyType: e.keys,
          ...e.content,
          minScore: e.content.minScore.withNull(false),
        }
      })
  })
  .pipe((aggrateView) => {
    return companyTableDefine
      // 由于声明了join的withNull为true
      // 所以minScore的withNull变回true,
      // maxScore的withNull会由boolean变为true,但查询结果的推断类型相同
      .join('lazy', true, aggrateView, ({ ref }) => `${ref((e) => e.base.companyType)} = ${ref((e) => e.extra.companyType)}`)
  })

//适配postgres
const creator = new RawSqlCreator({
  paramHolder: (index) => `$${index + 1}`,
  skip: 'offset',
  take: 'limit',
})


/** 
{
  sql: 'select "table_0"."column_a" as "value_0","table_0"."column_b" as "value_1","table_0"."column_c" as "value_2" from "public"."tableName1" as "table_0"',
  params: [],
  rawFormatter: [Function: rawFormatter]
}
*/
console.log(creator.selectAll(view.mapTo((e) => ({
  ...e.base
}))))


/**
{
  sql: 'select "table_0"."column_c" as "value_0","table_0"."column_b" as "value_1","table_1"."value_0" as "value_2" from "public"."tableName1" as "table_0" left join (select max("table_2"."column_c") as "value_0","table_3"."column_b" as "value_1" from "public"."tableName2" as "table_2" left join "public"."tableName1" as "table_3" on "table_2"."column_a" = "table_3"."column_a" group by "table_3"."column_b" having min("table_2"."column_c") > $1) as "table_1" on "table_0"."column_b" = "table_1"."value_1"',
  params: [ 0 ],
  rawFormatter: [Function: rawFormatter]
}
 */
console.log(creator.selectAll(view.mapTo((e) => ({
  companyName: e.base.name,
  companyType: e.base.companyType,
  maxScore: e.extra.maxScore.format((v) => Number(v)),
  // minScore:e.extra.minScore,
}))))


const executor = new SqlExecutor(
  //适配mysql
  new RawSqlCreator({
    paramHolder: (index) => `:param${index}`,
    skip: 'skip',
    take: 'take',
  }), async (sql, params) => {
    console.log({ sql, params })

    // 使用其他工具进行query
    // ...
    return []
  })


/**
{
  sql: 'select "table_0"."column_a" as "value_0","table_0"."column_b" as "value_1","table_0"."column_c" as "value_2" from "public"."tableName1" as "table_0"',
  params: []
}
*/
executor.selectAll(view.mapTo((e) => ({
  ...e.base
}))).then((arr) => {
  type Arr = {
    readonly companyId: string;
    readonly companyType: string;
    readonly name: string;
  }[]
})

/**
{
  sql: 'select "table_0"."column_c" as "value_0","table_0"."column_b" as "value_1","table_1"."value_0" as "value_2" from "public"."tableName1" as "table_0" left join (select max("table_2"."column_c") as "value_0","table_3"."column_b" as "value_1" from "public"."tableName2" as "table_2" left join "public"."tableName1" as "table_3" on "table_2"."column_a" = "table_3"."column_a" group by "table_3"."column_b" having min("table_2"."column_c") > :param0) as "table_1" on "table_0"."column_b" = "table_1"."value_1"',
  params: [ 0 ]
}
*/
executor.selectAll(view.mapTo((e) => ({
  companyName: e.base.name,
  companyType: e.base.companyType,
  maxScore: e.extra.maxScore.format((v) => Number(v)),
  // minScore:e.extra.minScore,
}))).then((arr) => {
  type Arr = {
    readonly companyName: string;
    readonly companyType: string;
    readonly maxScore: number | null;
  }[]
})

```

# 导出

## method
+ createFromDefine

## class
+ RawSqlCreator
+ SqlExecutor

## type
+ SqlView
+ Column
+ ColumnDeclareFun
+ SqlViewTemplate
+ GetRefStr

# 其他说明

- `andWhere`中了一个`ref`,`param`以及`select1From`方法 <br/>`select1From`方法主要用来进行`exists`查询，但这个设计是否合理没太想好

- `join`的`lazy`模式会在最终查询前分析哪些`extra`中的`Column`被用到（包括在`query`，`andWhere`或是其他情况）,没有用到则不会进行联查，所以使用`lazy`模式时，需要确定*无论是否进行join，都不会对当前查询结果的数量产生影响*

- `groupBy`可以使用多个key,可以传入数组，对象或更深层的嵌套结构W
