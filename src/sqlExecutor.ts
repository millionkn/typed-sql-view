import { exec, SqlViewTemplate, Adapter, createResolver, hasOneOf, Column, SqlViewTemplateCtx } from "./tools.js";
import { BuildFlag, SelectResult, SqlView } from "./sqlView.js";

export class SqlExecutor<RCtx> {
	static createMySqlExecutor<RCtx = unknown>(opts: {
		runner: (sql: string, params: unknown[], ctx: RCtx) => Promise<{ [key: string]: unknown }[]>
	}) {
		return new SqlExecutor<RCtx>({
			runner: (sql, params, ctx) => opts.runner(sql, params, ctx),
			adapter: {
				paramHolder: () => `?`,
				skip: (v) => `offset ${v}`,
				take: (v) => `limit ${v}`,
			},
		})
	}
	static createPostgresExecutor<RCtx = unknown>(opts: {
		runner: (sql: string, params: unknown[], ctx: RCtx) => Promise<{ [key: string]: unknown }[]>
	}) {
		return new SqlExecutor<RCtx>({
			runner: (sql, params, ctx) => opts.runner(sql, params, ctx),
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
			runner: (sql: string, params: unknown[], ctx: RCtx) => Promise<{ [key: string]: unknown }[]>
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
		ctx: RCtx & SqlViewTemplateCtx<NoInfer<VT2>>
	) {
		return view
			.bracketIf((sqlBody) => hasOneOf(sqlBody.state(), ['groupBy', 'skip', 'take']))
			.mapTo((e) => getTemplate(e))
			.pipe(async (view) => {
				const rawSelect = this.rawSelectAll(view, { order: false })
				return this.opts.runner(rawSelect.sql, rawSelect.paramArr, ctx).then(([raw]) => {
					if (!raw) { throw new Error('aggrate no result') }
					return rawSelect.rawFormatter(raw, ctx)
				})
			})
	}

	async selectAll<VT extends SqlViewTemplate>(view: SqlView<VT>, ctx: RCtx & SqlViewTemplateCtx<NoInfer<VT>>): Promise<SelectResult<VT>[]> {
		const rawSql = this.rawSelectAll(view, { order: true })
		return this.opts.runner(rawSql.sql, rawSql.paramArr, ctx).then((arr) => Promise.all(arr.map((raw) => rawSql.rawFormatter(raw, ctx))))
	}

	async selectOne<VT extends SqlViewTemplate>(view: SqlView<VT>, ctx: RCtx & SqlViewTemplateCtx<NoInfer<VT>>) {
		return this.selectAll(view.take(1), ctx).then((arr) => arr[0] ?? null)
	}

	async getTotal<VT extends SqlViewTemplate>(view: SqlView<VT>, ctx: RCtx) {
		return this.aggrateView(view, () => Column.create(`count(*)`).format((raw) => Number(raw)).withNull(false), ctx)
	}

	async query<VT extends SqlViewTemplate>(withCount: boolean, page: null | { take: number, skip: number }, view: SqlView<VT>, ctx: RCtx & SqlViewTemplateCtx<NoInfer<VT>>) {
		return Promise.all([
			this.selectAll(view.skip(page?.skip).take(page?.take), ctx),
			withCount ? this.getTotal(view, ctx) : -1,
		]).then(([data, total]) => ({ data, total }))
	}
}

