import { SqlViewTemplate, Column, SyntaxAdapter, parseAndEffectOnHelper, BuildSqlHelper, ParamType, sql, Segment, sym } from "./define.js";
import { BuildFlag, SelectResult, SqlView } from "./sqlView.js";
import { exec, hasOneOf, iterateTemplate } from "./tools.js";

export class SqlAdapter {
	static createMySqlAdapter() {
		return new SqlAdapter({
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
	static createPostgresAdapter() {
		return new SqlAdapter({
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
	constructor(
		private opts: {
			paramHolder: (index: number) => string,
			adapter: SyntaxAdapter
		}
	) { }

	private createRawSelectAll<VT extends SqlViewTemplate>(
		view: SqlView<VT>,
		flag: BuildFlag,
	) {
		const paramArr: unknown[] = []
		const helper: BuildSqlHelper = {
			adapter: this.opts.adapter,
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
					const result = this.opts.paramHolder(paramArr.length)
					paramArr.push(param)
					return result
				}
			}),
		}
		const builder = view[sym].createStructBuilder()
		iterateTemplate(builder.template, (c) => c instanceof Column, (c) => {
			Column.getOpts(c).builderCtx.emitInnerUsed()
		})
		builder.emitInnerUsed()
		const sqlBody = builder.finalize(flag)
		const bodyExprStr = parseAndEffectOnHelper(sqlBody.buildBodyExpr(), helper)
		const selectMapper: Map<string, { aliasStr: string }> = new Map()
		const formatMapper: Map<Column, (raw: { [key: string]: unknown }) => Promise<unknown>> = new Map()
		iterateTemplate(builder.template, (c) => c instanceof Column, (c) => {
			const columnOpts = Column.getOpts(c)
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
				const resultMapper = new Map<Column, unknown>()
				formatMapper.forEach((formatter, c) => {
					const promise = formatter(raw)
					loadingArr.push(promise.then((v) => {
						resultMapper.set(c, v)
					}))
				})
				await Promise.all(loadingArr)
				return iterateTemplate(builder.template, (c) => c instanceof Column, (c) => resultMapper.get(c)!) as SelectResult<VT>
			}
		}
	}


	aggrateView<VT1 extends SqlViewTemplate, VT2 extends SqlViewTemplate>(
		view: SqlView<VT1>,
		getTemplate: (createColumn: (getSegment: (vt: VT1) => Segment) => Column) => VT2,
	) {
		const rawSelect = view
			.bracketIf((opts) => hasOneOf(opts.state, ['groupBy', 'skip', 'take']))
			.mapTo((e, createColumn) => getTemplate((getSegment) => createColumn(getSegment(e))))
			.pipe((view) => this.createRawSelectAll(view, { order: false }))

		return {
			sql: rawSelect.sql,
			paramArr: rawSelect.paramArr,
			formatter: (arr: { [key: string]: unknown }[]) => {
				if (arr.length === 0) { throw new Error('aggrate no result') }
				return rawSelect.rawFormatter(arr[0])
			},
		}
	}

	selectAll<VT extends SqlViewTemplate>(
		view: SqlView<VT>,
	) {
		const rawSelect = this.createRawSelectAll(view, { order: true })
		return {
			sql: rawSelect.sql,
			paramArr: rawSelect.paramArr,
			formatter: (arr: { [key: string]: unknown }[]) => Promise.all(arr.map((raw) => rawSelect.rawFormatter(raw))),
		}
	}

	selectOne<VT extends SqlViewTemplate>(
		view: SqlView<VT>
	) {
		const rawSelect = this.createRawSelectAll(view.take(1), { order: true })
		return {
			sql: rawSelect.sql,
			paramArr: rawSelect.paramArr,
			formatter: (arr: { [key: string]: unknown }[]) => rawSelect.rawFormatter(arr[0]),
		}
	}

	selectTotal<VT extends SqlViewTemplate>(view: SqlView<VT>) {
		return this.aggrateView(view, (createColumn) => createColumn(() => sql`count(*)`).format((raw) => Number(raw)).withNull(false))
	}
}

