import { SqlExecutor, createSqlView, createSqlViewFromTable, } from '../src/index.js'
import z from 'zod'
import { createColumn } from '../src/define.js'


const vvv = createSqlViewFromTable(sql`public.tableName1`, (rootAlias) => {
	const companyId = createColumn(sql`"${rootAlias}"."column_a"`).withNull(false)
	return {
		companyId: companyId.format((raw) => z.string().parse(raw)),
		companyIdIsTarget: createColumn(sql`${companyId} = ${'123456'}`).withNull(false)
	}
})

const companyTableDefine = createSqlViewFromTable(sql`"public"."tableName1"`, (rootAlias) => {
	return {
		companyId: createColumn(sql`"${rootAlias}"."column_a"`)
			.withNull(false)
			.format((raw) => z.string().parse(raw)),
		companyType: createColumn(sql`"${rootAlias}"."column_b"`)
			.withNull(false)
			//format支持异步
			.format(async (raw) => z.string().parse(raw)),
		name: createColumn(sql`"${rootAlias}"."column_c"`)
			.withNull(false)
			.format((raw) => z.string().parse(raw)),
	}
}).andWhere((e) => sql`${e.companyType} like ${`%type%`}`)
	.andWhere((e) => sql`exists (${vvv
		.andWhere((e2) => sql`${e2.companyId} = ${e.companyId}`)
		.skip(1)
		.mapTo(() => [])}
	)`)
	.lateralJoin('lazy left with null', (e) => {
		return createSqlViewFromTable(sql`public.tableName2`, (rootAlias) => {
			return {
				companyId: createColumn(sql`"${rootAlias}"."column_a"`).withNull(false)
			}
		}).andWhere((e2) => sql`${e2.companyId} = ${e.base.companyId}`)
	})
	.on((e) => sql`${e.base.companyId} = ${e.extra.companyId}`)
	.mapTo((e) => {
		return {
			...e.base,
			xx: createColumn(sql`${e.base.companyId}`)
		}
	})
	.decalreUsed((e) => e)

const personTableDefine = createSqlView(({ addFrom }) => {
	const alias = addFrom(`"public"."tableName2"`)
	const companyId = createColumn(`"${alias}"."column_a"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw))
	const identify = createColumn(`"${alias}"."column_b"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw))
	return {
		companyId,
		identify,
		primaryKeys: [companyId, identify],

		//可以延迟设置 'format' 与 'withNull'
		scoreValue: createColumn(`"${alias}"."column_c"`),
		name: createColumn(`"${alias}"."column_d"`).withNull(false).format((raw) => z.string().transform((v) => String(v)).parse(raw)),
		jsonTag: createColumn(`"${alias}"."column_e"`).withNull(false),
	}
})

const _view = personTableDefine
	.join((e, { leftJoin }) => {
		const otherTable = leftJoin(false, personTableDefine, (t) => `${t.identify} = ${e.identify}`)
		return {
			...e,
			flatedTag: otherTable.jsonTag
		}
	})
await SqlExecutor.createPostgresExecutor({
	runner: async (sql, params) => {
		console.log({ sql, params })
		return []
	}
}).selectAll(_view)

const view = _view
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
const executor = SqlExecutor.createMySqlExecutor({
	runner: async (sql, params) => {
		// 使用其他工具进行query
		// console.log(sql, params)
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
		// console.log({ sql, params })
		return []
	}
}).selectAll(view.skip(1).take(5).mapTo((e) => ({
	...e.base,
	...e.extra,
})))