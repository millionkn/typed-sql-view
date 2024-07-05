import { Adapter, SqlExecutor, createFromDefine } from '../src/index.js'
import z from 'zod'

const companyTableDefine = createFromDefine(`"public"."tableName1"`, (define) => {
  return {
    companyId: define((alias) => `"${alias}"."column_a"`).assert('', 'companyId').withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
    companyType: define((alias) => `"${alias}"."column_b"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
    name: define((alias) => `"${alias}"."column_c"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
  }
})

const personTableDefine = createFromDefine(`"public"."tableName2"`, (define) => {
  const companyId = define((alias) => `"${alias}"."column_a"`).assert('', `companyId`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw))
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
// adapter = Adapter.postgresAdapter
const adapter = new Adapter({
  language: {
    skip: 'offset',
    take: 'limit',
  },
  createParamCtx: () => {
    const result: unknown[] = []
    return {
      getParamResult: () => result,
      setParam: (value, index) => {
        result.push(value)
        return { holder: `$${index + 1}` }
      },
    }
  },

})


/** 
{
  sql: 'select "table_0"."column_a" as "value_0","table_0"."column_b" as "value_1","table_0"."column_c" as "value_2" from "public"."tableName1" as "table_0"',
  params: [],
  rawFormatter: [Function: rawFormatter]
}
*/
console.log(adapter.selectAll(view.mapTo((e) => ({
  ...e.base
}))))


/**
{
  sql: 'select "table_0"."column_c" as "value_0","table_0"."column_b" as "value_1","table_1"."value_0" as "value_2" from "public"."tableName1" as "table_0" left join (select max("table_2"."column_c") as "value_0","table_3"."column_b" as "value_1" from "public"."tableName2" as "table_2" left join "public"."tableName1" as "table_3" on "table_2"."column_a" = "table_3"."column_a" group by "table_3"."column_b" having min("table_2"."column_c") > $1) as "table_1" on "table_0"."column_b" = "table_1"."value_1"',
  params: [ 0 ],
  rawFormatter: [Function: rawFormatter]
}
 */
console.log(adapter.selectAll(view.mapTo((e) => ({
  companyName: e.base.name,
  companyType: e.base.companyType,
  maxScore: e.extra.maxScore.format((v) => Number(v)),
  // minScore:e.extra.minScore,
}))))


const executor = new SqlExecutor({
  //适配mysql,也可自行实现
  adapter: Adapter.mysqlAdapter,
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

console.log(createFromDefine(`"public"."tableName3"`, (define) => {
  return {
    id: define((alias) => `"${alias}"."column_a"`).assert('', 'companyId').withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
    name: define((alias) => `"${alias}"."column_b"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
    targetArr: define((alias) => `"${alias}"."column_c"`).withNull(false).assert('', 'str split by ","'),
  }
}).forceMapTo((e, c) => {
  return {
    id: e.id,
    name: e.name,
    target: c((ref) => `regexp_split_to_table(${ref(e.targetArr.assert('str split by ","'))}, ',')`).withNull(false).format((raw) => String(raw))
  }
})
  .andWhere(({ ref, param }) => `${ref((e) => e.target)} = ${param('zzz')}`)
  .pipe((view) => {
    return Adapter.postgresAdapter.selectAll(view)
  }))

class ElectAccidentEntity {
  static sqlView() {
    return createFromDefine(`"elect_accident_entity"`, (c) => {
      return {
        id: c((r) => `"${r}"."id"`).withNull(false).assert('', 'id').format((raw) => z.string().parse(raw)),
        所属大洲: c((r) => `"${r}"."所属大洲"`).withNull(false).assert('', 'string').format((raw) => z.string().parse(raw)),
        地点: c((r) => `"${r}"."地点"`).withNull(false).assert('', 'string').format((raw) => z.string().parse(raw)),
      }
    })
  }
}
ElectAccidentEntity.sqlView()
  .order('desc', (ref) => ref((e) => e.id))
  .forceMapTo((e, c) => {
    return {
      ...e,
      地点: c((ref) => `regexp_split_to_table(${ref(e.地点)}, '、')`).withNull(false).format((raw) => z.string().parse(raw)),
    }
  })
  .pipe((view) => Adapter.postgresAdapter.selectAll(view.mapTo((e) => {
    return {
      target: e.id.format((raw) => ({
        id: z.string().parse(raw),
      })),
      所属大洲: e.所属大洲,
      地点: e.地点,
    }
  })))