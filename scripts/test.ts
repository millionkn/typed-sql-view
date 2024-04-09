import { RawSqlCreator, SqlExecutor, createFromDefine } from '../src/index.js'
import z from 'zod'

const companyTableDefine = createFromDefine(`"public"."tableName1"`, (define) => {
  return {
    companyId: define((alias) => `"${alias}"."column_a"`).assert(null, 'companyId').withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
    companyType: define((alias) => `"${alias}"."column_b"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
    name: define((alias) => `"${alias}"."column_c"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
  }
})

const personTableDefine = createFromDefine(`"public"."tableName2"`, (define) => {
  const companyId = define((alias) => `"${alias}"."column_a"`).assert(null, `companyId`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw))
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
  //此处使用tag验证标签,一定程度上避免引用错误(可选)
  .join('lazy', true, companyTableDefine, ({ ref }) => `${ref((e) => e.base.companyId.assert('companyId', 'companyId'))} = ${ref((e) => e.extra.companyId.assert('companyId', 'companyId'))}`)
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


const executor = new SqlExecutor({
  //适配mysql
  creator: new RawSqlCreator({
    paramHolder: (index) => `:param${index}`,
    skip: 'skip',
    take: 'take',
  }),
  runner: async (sql, params) => {
    console.log({ sql, params })
    // 使用其他工具进行query
    // ...
    return []
  }
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