import { SqlViewTemplate, ColumnRef, SyntaxAdapter, parseAndEffectOnHelper, BuildSqlHelper, ParamType, sql, Segment, sym, SqlExecuteBundle } from "./define.js";
import { BuildFlag, SelectResult, SqlView } from "./sqlView.js";
import { exec, hasOneOf, iterateTemplate } from "./tools.js";

export interface SqlAdapter {
	selectAll: <VT extends SqlViewTemplate>(view: SqlView<VT>) => SqlExecuteBundle<SelectResult<VT>[]>
	selectOne: <VT extends SqlViewTemplate>(view: SqlView<VT>) => SqlExecuteBundle<null | SelectResult<VT>>
	selectTotal: <VT extends SqlViewTemplate>(view: SqlView<VT>) => SqlExecuteBundle<number>
	aggrateView: <VT1 extends SqlViewTemplate, VT2 extends SqlViewTemplate>(view: SqlView<VT1>, getTemplate: (createColumn: (getSegment: (vt: VT1) => Segment) => ColumnRef) => VT2) => SqlExecuteBundle<SelectResult<VT2>>
}

export function createSqlAdapter(opts: {
	paramHolder: (index: number) => string,
	adapter: SyntaxAdapter
}): SqlAdapter {

	const createRawSelectAll = <VT extends SqlViewTemplate>(
		view: SqlView<VT>,
		flag: BuildFlag,
	) => {
		const paramArr: unknown[] = []
		const helper: BuildSqlHelper = {
			adapter: opts.adapter,
			fetchColumnAlias: exec(() => {
				const resultMapper = new Map<symbol | string, string>()
				return (key) => {
					if (!resultMapper.has(key)) {
						const result = `column_${resultMapper.size}`
						resultMapper.set(key, result)
					}
					return resultMapper.get(key)!
				}
			}),
			fetchTableAlias: exec(() => {
				const resultMapper = new Map<symbol | string, string>()
				return (key) => {
					if (!resultMapper.has(key)) {
						const result = `table_${resultMapper.size}`
						resultMapper.set(key, result)
					}
					return resultMapper.get(key)!
				}
			}),
			setParam: exec(() => {
				return (param: ParamType) => {
					if (!(param instanceof Array)) { param = [param] }
					return param.map((param) => {
						const result = opts.paramHolder(paramArr.length)
						paramArr.push(param)
						return result
					}).join(',')
				}
			}),
		}
		const builder = view[sym].createStructBuilder()
		iterateTemplate(builder.template, (c) => c instanceof ColumnRef, (c) => {
			ColumnRef.getOpts(c).builderCtx.emitInnerUsed()
		})
		builder.emitInnerUsed()
		const sqlBody = builder.finalize(flag)
		const bodyExprStr = parseAndEffectOnHelper(sqlBody.buildBodyExpr(), helper)
		const selectMapper: Map<string, { aliasStr: string }> = new Map()
		const formatMapper: Map<ColumnRef, (raw: { [key: string]: unknown }) => Promise<unknown>> = new Map()
		iterateTemplate(builder.template, (c) => c instanceof ColumnRef, (c) => {
			const columnOpts = ColumnRef.getOpts(c)
			const selectStr = parseAndEffectOnHelper(columnOpts.builderCtx.buildExpr(), helper)
			const aliasStr = helper.fetchColumnAlias(selectStr)
			formatMapper.set(c, async (raw) => {
				if (columnOpts.withNull === true && raw[aliasStr] === null) { return null }
				return columnOpts.format(raw[aliasStr])
			})
			if (!selectMapper.has(selectStr)) {
				selectMapper.set(selectStr, {
					aliasStr,
				})
			}
		})

		return {
			sql: ([
				'select ',
				selectMapper.size === 0 ? '1' : [...selectMapper.entries()].map(([selectStr, { aliasStr }]): string => {
					return helper.adapter.selectAndAlias(selectStr, aliasStr)
				}).join(','),
				' from ',
				bodyExprStr,
			] satisfies string[]).join(''),
			paramArr,
			rawFormatter: async (raw: { [key: string]: unknown }) => {
				const loadingArr: Promise<unknown>[] = new Array()
				const resultMapper = new Map<ColumnRef, unknown>()
				formatMapper.forEach((formatter, c) => {
					const promise = formatter(raw)
					loadingArr.push(promise.then((v) => {
						resultMapper.set(c, v)
					}))
				})
				await Promise.all(loadingArr)
				return iterateTemplate(builder.template, (c) => c instanceof ColumnRef, (c) => resultMapper.get(c)!) as SelectResult<VT>
			}
		}
	}

	const aggrateView: SqlAdapter['aggrateView'] = (view, getTemplate) => {
		const rawSelect = view
			.bracketIf((opts) => hasOneOf(opts.state, ['groupBy', 'skip', 'take']))
			.mapTo((e, { createColumn }) => getTemplate((getSegment) => createColumn(getSegment(e))))
			.pipe((view) => createRawSelectAll(view, { order: false }))
		return {
			sql: rawSelect.sql,
			paramArr: rawSelect.paramArr,
			formatter: (arr: { [key: string]: unknown }[]) => {
				if (arr.length === 0) { throw new Error('aggrate no result') }
				return rawSelect.rawFormatter(arr[0])
			},
		}
	}
	const selectAll: SqlAdapter['selectAll'] = (view) => {
		const rawSelect = createRawSelectAll(view, { order: true })
		return {
			sql: rawSelect.sql,
			paramArr: rawSelect.paramArr,
			formatter: (arr) => Promise.all(arr.map((raw) => rawSelect.rawFormatter(raw))),
		}
	}
	const selectOne: SqlAdapter['selectOne'] = (view) => {
		const rawSelect = createRawSelectAll(view.take(1), { order: true })
		return {
			sql: rawSelect.sql,
			paramArr: rawSelect.paramArr,
			formatter: async (arr) => arr.length === 0 ? null : rawSelect.rawFormatter(arr[0]),
		}
	}
	const selectTotal: SqlAdapter['selectTotal'] = (view) => {
		return aggrateView(view, (createColumn) => createColumn(() => sql`count(*)`).format((raw) => Number(raw)).withNull(false))
	}

	return {
		aggrateView: aggrateView,
		selectAll: selectAll,
		selectOne: selectOne,
		selectTotal: selectTotal,
	}
}

export function createSqlAdapterForMysql() {

	return createSqlAdapter({
		paramHolder: () => `?`,
		adapter: {
			tableAlias: (alias) => `"${alias}"`,
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
	})
}
export function createSqlAdapterForPostgres() {
	return createSqlAdapter({
		paramHolder: (index) => `$${index + 1}`,
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
	})
}

