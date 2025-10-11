import { SqlViewTemplate, Column, SyntaxAdapter } from "./define.js";
import { SelectResult, SqlView } from "./sqlView.js";
import { hasOneOf } from "./tools.js";

export class SqlExecutor {
	static createMySqlExecutor(opts: {
		runner: (sql: string, params: unknown[]) => Promise<{ [key: string]: unknown }[]>
	}) {
		return new SqlExecutor({
			runner: (sql, params) => opts.runner(sql, params),
			adapter: {
				selectAndAlias: (select, alias) => `${select} as "${alias}"`,
				paramHolder: () => `?`,
				skip: (v) => `offset ${v}`,
				take: (v) => `limit ${v}`,
			},
		})
	}
	static createPostgresExecutor(opts: {
		runner: (sql: string, params: unknown[]) => Promise<{ [key: string]: unknown }[]>
	}) {
		return new SqlExecutor({
			runner: (sql, params) => opts.runner(sql, params),
			adapter: {
				selectAndAlias: (select, alias) => `${select} as "${alias}"`,
				paramHolder: (index) => `$${index + 1}`,
				skip: (v) => `offset ${v}`,
				take: (v) => `limit ${v}`,
			},
		})
	}
	constructor(
		private opts: {
			adapter: SyntaxAdapter & {
				paramHolder: (index: number) => string,
			},
			runner: (sql: string, params: unknown[]) => Promise<{ [key: string]: unknown }[]>
		}
	) { }


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

	async selectAll<VT extends SqlViewTemplate>(
		view: SqlView<VT>,
	): Promise<SelectResult<VT>[]> {
		const rawSql = this.rawSelectAll(view, { order: true })
		return this.opts.runner(rawSql.sql, rawSql.paramArr).then((arr) => Promise.all(arr.map((raw) => rawSql.rawFormatter(raw))))
	}

	async selectOne<VT extends SqlViewTemplate>(
		view: SqlView<VT>
	) {
		return this.selectAll(view.take(1)).then((arr) => arr[0] ?? null)
	}

	async getTotal<VT extends SqlViewTemplate>(view: SqlView<VT>): Promise<number> {
		return this.aggrateView(view, () => Column.create(`count(*)`).format((raw) => Number(raw)).withNull(false))
	}

	async query<VT extends SqlViewTemplate>(
		withCount: boolean,
		page: null | { take: number, skip: number },
		view: SqlView<VT>,
	) {
		return Promise.all([
			this.selectAll(view.skip(page?.skip).take(page?.take)),
			withCount ? this.getTotal(view) : -1,
		]).then(([data, total]) => ({ data, total }))
	}
}

