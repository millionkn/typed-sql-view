import { Adapter, SqlExecutor, createSqlView, Column, SqlView, } from '../src/index.js'
import z from 'zod'
import { createColumn } from '../src/tools.js'

const companyTableDefine = createSqlView(({ addFrom }) => {
	const alias = addFrom(`"public"."tableName1"`)
	return {
		companyId: createColumn(`"${alias}"."column_a"`).assert('', 'companyId').withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
		companyType: createColumn(`"${alias}"."column_b"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
		name: createColumn(`"${alias}"."column_c"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
	}
})

const personTableDefine = createSqlView(({ addFrom }) => {
	const alias = addFrom(`"public"."tableName2"`)
	const companyId = createColumn(`"${alias}"."column_a"`).assert('', `companyId`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw))
	const identify = createColumn(`"${alias}"."column_b"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw))
	return {
		companyId,
		identify,
		primaryKeys: [companyId, identify],

		//可以延迟设置 'format' 与 'withNull'
		scoreValue: createColumn(`"${alias}"."column_c"`),
		name: createColumn(`"${alias}"."column_d"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
	}
})

const view = personTableDefine
	//根据业务要求,知道person最多有一个company,无论是否join 'company表'都不会影响'person表'的数量
	//所以可以使用lazy,如果后面没有用到,就不进行join
	//ps:left和inner都会立刻join,因为表的数量可能会被影响
	//此处使用tag验证标签,一定程度上避免引用错误(可选)
	.joinUnstable((e, { lazyJoin }) => {
		return {
			person: e,
			company: lazyJoin(true, companyTableDefine, (t) => `${e.companyId} = ${t.companyId}`),
		}
	})
	.pipe((view) => {
		return view
			.groupBy((e) => e.company.companyType, (e) => {
				return {
					//由于后面没有使用minScore,所以实际不会被选择
					minScore: createColumn(`min(${e.person.scoreValue})`),
					maxScore: createColumn(`max(${e.person.scoreValue})`),
				}
			})
			//这里由于groupBy了,所以会使用'having'而不是'where'
			.andWhere((e,param) => `${ e.content.minScore} > ${param(0)}`)
			//根据过滤条件，确定结果不会为null
			.mapTo((e) => {
				return {
					companyType: e.keys,
					...e.content,
					minScore: e.content.minScore.withNull(false),
				}
			})
	})
	.pipe((view) => companyTableDefine.joinUnstable((base,{lazyJoin}) => {
		return {
			base,
			// 由于声明了join的withNull为true
			// 所以minScore的withNull变回true,
			// maxScore的withNull会由boolean变为true,但查询结果的推断类型相同
			extra:lazyJoin(true,view,(t)=>`${t.companyType} = ${base.companyType}`)
		}
	}))


//适配mysql,也可自行实现 
const executor = SqlExecutor.createMySqlExecutor(async (sql, params) => {
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
	...e
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
// executor.selectAll(view.mapTo((e) => ({
// 	companyName: e.base.name,
// 	companyType: e.base.companyType,
// 	maxScore: e.extra.maxScore.format((v) => Number(v)),
// 	// minScore:e.extra.minScore,
// }))).then((arr) => {
// 	type Arr = {
// 		readonly companyName: string;
// 		readonly companyType: string;
// 		readonly maxScore: number | null;
// 	}[]
// })

