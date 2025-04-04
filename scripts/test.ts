import { SqlExecutor, createSqlView, } from '../src/index.js'
import z from 'zod'
import { createColumn } from '../src/tools.js'

const companyTableDefine = createSqlView(({ addFrom }) => {
	const alias = addFrom(`"public"."tableName1"`)
	return {
		companyId: createColumn(`"${alias}"."column_a"`).assert('', 'companyId').withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
		companyType: createColumn(`"${alias}"."column_b"`)
			.withNull(false)
			//format支持异步
			.format(async (raw) => z.string().parse(raw)),
		name: createColumn(`"${alias}"."column_c"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
	}
}).andWhere((e, param) => `${e.companyType} like ${param(`%type%`)}`)

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
	.joinLazy((e, { leftJoin }) => {
		return {
			person: e,
			company: leftJoin(true, companyTableDefine, (t) => `${e.companyId} = ${t.companyId}`),
		}
	})
	.pipe((view) => {
		return view
			.groupBy((e) => [e.company.companyType], (e) => {
				return {
					companyType: e.company.companyType,
					//由于后面没有使用minScore,所以实际不会被选择
					minScore: createColumn(`min(${e.person.scoreValue})`),
					maxScore: createColumn(`max(${e.person.scoreValue})`),
				}
			})
			//这里由于groupBy了,所以会使用'having'而不是'where'
			.andWhere((e, param) => `${e.minScore} > ${param(0)}`)
			//根据过滤条件，确定结果不会为null
			.mapTo((e) => {
				return {
					...e,
					minScore: e.minScore.withNull(false),
				}
			})
	})
	.pipe((view) => companyTableDefine.joinLazy((base, { leftJoin }) => {
		return {
			base,
			// 由于声明了join的withNull为true
			// 所以minScore的withNull变回true,
			// maxScore的withNull会由boolean变为true,但查询结果的推断类型相同
			extra: leftJoin(true, view, (t) => `${t.companyType} = ${base.companyType}`)
		}
	}))


//适配mysql,也可自行实现 
const executor = SqlExecutor.createMySqlExecutor<{
	runner: (sql: string, params: any[]) => Promise<any[]>
}>({
	runner: async (sql, params) => {
		// 使用其他工具进行query
		console.log(sql, params)
		return []
	}
})



/**
{
	sql: 'select "table_0"."column_a" as "value_0","table_0"."column_b" as "value_1","table_0"."column_c" as "value_2" from "public"."tableName1" as "table_0"',
	params: []
}
*/


executor.selectAll(view.skip(1).take(5).mapTo((e) => {
	return e.base
})).then((arr) => {
	const typeCheck = arr satisfies {
		companyId: string;
		companyType: string;
		name: string;
	}[]
})

SqlExecutor.createPostgresExecutor({
	runner: async (sql, params) => {
		console.log({ sql, params })
		return []
	}
}).selectAll(view.skip(1).take(5).mapTo((e) => ({
	...e.base,
	...e.extra,
})))