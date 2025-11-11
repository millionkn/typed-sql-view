import { createSqlAdapter, createSqlView, sql } from '../src/index.js'
import z from 'zod'


const personTableDefine = createSqlView(sql`"public"."personTable"`, (column) => {
	return {
		companyId: column((rootAlias) => sql`"${rootAlias}"."companyId"`).withNull(false).format((raw) => z.string().parse(raw)),
		//format支持异步
		personName: column((rootAlias) => sql`"${rootAlias}"."personName"`).withNull(false).format(async (raw) => z.string().parse(raw)),

	}
})

const companyTableDefine = createSqlView(sql`"public"."companyTable"`, (column) => {
	return {
		companyId: column((rootAlias) => sql`"${rootAlias}"."companyId"`)
			.withNull(false)
			.format((raw) => z.string().parse(raw)),
		companyName: column((rootAlias) => sql`"${rootAlias}"."name"`)
			.withNull(false)
			.format((raw) => z.string().parse(raw)),
	}
})

const view = companyTableDefine
	.andWhere((e) => sql`${e.companyName} like ${`%companyName%`}`)
	.andWhere(() => sql`exists (${personTableDefine
		.andWhere((e2) => sql`${e2.personName} = ${'targetPersonName'}`)
		// .mapTo(() => [])
		})`)
	.lateralJoin('left join lazy withNull', (e) => {
		return personTableDefine
			.andWhere((e2) => sql`${e2.companyId} = ${e.companyId}`)
			.groupBy((e2) => ({ companyId: e2.companyId }), (column) => {
				return {
					count: column(() => sql`count(*)`).withNull(false).format((raw) => z.number().parse(raw)),
				}
			})
			.andWhere((e2) => sql`${e2.aggrateValues.count} > ${5}`)
	})
	.on((e) => sql`${e.base.companyId} = ${e.extra.keys.companyId}`)
// .mapTo((e) => e.base)


const selectAll = createSqlAdapter({
	paramHolder: (index) => {
		return `$${index + 1}`
	},
	adapter: {
		tableAlias: (alias) => `as "${alias}"`,
		columnRef: (tableAlias, columnAlias) => `"${tableAlias}"."${columnAlias}"`,
		selectAndAlias: (select, alias) => `${select} as "${alias}"`,
		pagination: (skip, take) => {
			let result: string[] = []
			if (skip > 0) { result.push(`offset ${skip}`) }
			if (take !== null) { result.push(`limit ${take}`) }
			return result.join(' ')
		},
		order: (items) => `order by ${items.map(({ expr, order, nulls }) => `${expr} ${order} NULLS ${nulls}`).join(',')}`,
	},
}).selectAll(view)
console.log('=========sql==========')
console.log(selectAll.sql)
console.log('=========paramArr==========')
console.log(selectAll.paramArr)
console.log('=========end==========')

