import { exec, SqlViewTemplate, Adapter, createResolver, hasOneOf, Column } from "./tools.js";
import { BuildFlag, SqlView } from "./sqlView.js";

export class SqlExecutor {
	static createMySqlExecutor(runner: (sql: string, params: unknown[]) => Promise<{ [key: string]: unknown }[]>) {
		return new SqlExecutor({
			runner: (sql, params) => runner(sql, params),
			adapter: {
				paramHolder: () => `?`,
				skip: (v) => `offset ${v}`,
				take: (v) => `limit ${v}`,
			},
		})
	}
	static createPostgresExecutor(runner: (sql: string, params: unknown[]) => Promise<{ [key: string]: unknown }[]>) {
		return new SqlExecutor({
			runner: (sql, params) => runner(sql, params),
			adapter: {
				paramHolder: (index) => `$${index + 1}`,
				skip: (v) => `offset ${v}`,
				take: (v) => `limit ${v}`,
			},
		})
	}
	constructor(
		private opts: {
			adapter: Adapter,
			runner: (sql: string, params: unknown[]) => Promise<{ [key: string]: unknown }[]>
		}
	) { }

	private rawSelectAll<VT extends SqlViewTemplate>(
		view: SqlView<VT>,
		flag: BuildFlag,
	) {
		const resolver = exec(() => {
			const resolver = createResolver<string>()
			return {
				createHolder: (getValue: () => string) => {
					let saved = null as null | string
					return resolver.createHolder(() => saved ||= getValue())
				},
				resolve: (str: string) => {
					return resolver.resolve(str, (key) => {
						if (key.startsWith(`take__`)) {
							const value = Number(key.slice(`take__`.length))
							return this.opts.adapter.take(value)
						}
						if (key.startsWith(`skip__`)) {
							const value = Number(key.slice(`skip__`.length))
							return this.opts.adapter.skip(value)
						}
						throw new Error()
					}).map((e) => typeof e === 'string' ? e : e()).join('')
				}
			}
		})

		const paramArr = [] as unknown[]
		const viewResult = view.buildSelectAll({ order: flag.order }, {
			createHolder: () => {
				let getValue = (): string => { throw new Error(`expr:${expr}`) }
				const expr = resolver.createHolder(() => getValue())
				return {
					expr,
					replaceWith: (expr) => getValue = () => resolver.resolve(expr)
				}
			},
			genAlias: exec(() => {
				let index = 0
				return () => resolver.createHolder(() => `table_${index++}`)
			}),
			setParam: exec(() => {
				let index = 0
				return (value) => resolver.createHolder(() => {
					paramArr[index] = value
					return this.opts.adapter.paramHolder(index++)
				})
			}),
		})
		return {
			sql: resolver.resolve(viewResult.sql),
			paramArr: paramArr,
			rawFormatter: viewResult.rawFormatter,
		}
	}


	aggrateView<VT1 extends SqlViewTemplate, VT2 extends SqlViewTemplate>(
		view: SqlView<VT1>,
		getTemplate: (vt: VT1) => VT2,
	) {
		return view
			.bracketIf((sqlBody) => hasOneOf(sqlBody.state(), ['groupBy', 'skip', 'take']))
			.mapTo((e) => getTemplate(e))
			.pipe(async (view) => {
				const rawSelect = this.rawSelectAll(view, { order: false })
				return this.opts.runner(rawSelect.sql, rawSelect.paramArr).then(([raw]) => {
					if (!raw) { throw new Error('aggrate no result') }
					return rawSelect.rawFormatter(raw)
				})
			})
	}

	async selectAll<VT extends SqlViewTemplate>(view: SqlView<VT>) {
		const rawSql = this.rawSelectAll(view, { order: true })
		return this.opts.runner(rawSql.sql, rawSql.paramArr).then((arr) => arr.map((raw) => rawSql.rawFormatter(raw)))
	}

	async selectOne<VT extends SqlViewTemplate>(view: SqlView<VT>) {
		return this.selectAll(view.take(1)).then((arr) => arr[0] ?? null)
	}



	async getTotal(view: SqlView<SqlViewTemplate>) {
		return this.aggrateView(view, () => Column.create(`count(*)`).format((raw) => Number(raw)).withNull(false))
	}

	async query<VT extends SqlViewTemplate>(withCount: boolean, page: null | { take: number, skip: number }, view: SqlView<VT>) {
		return Promise.all([
			this.selectAll(view.skip(page?.skip).take(page?.take)),
			withCount ? this.getTotal(view) : -1,
		]).then(([data, total]) => ({ data, total }))
	}
}

