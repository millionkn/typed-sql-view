import { Async, exec } from "./tools.js"

export type DeepTemplate<I> = I | (readonly [...DeepTemplate<I>[]]) | { readonly [key: string]: DeepTemplate<I> }

export const iterateTemplate = (template: SqlViewTemplate, cb: (column: Column<boolean, unknown>) => unknown): unknown => {
	if (template instanceof Column) {
		return cb(template)
	} else if (template instanceof Array) {
		return template.map((t) => iterateTemplate(t, cb))
	} else {
		return Object.fromEntries(Object.entries(template).map(([key, t]) => [key, iterateTemplate(t, cb)]))
	}
}

type _ParamType = string | number | boolean | null | Date
type ParamType = _ParamType | Array<_ParamType>

type SegmentValue =
	| ParamType
	| SqlSegmentOwner

export type SqlSegmentData = {
	strings: string[],
	values: SegmentValue[],
}
export type SyntaxAdapter = {
	skip: (value: number) => string,
	take: (value: number) => string,
	paramHolder: (index: number) => string,
	delimitedIdentifiers: (identifier: string) => string,
	aliasAs: (alias: string, type: 'table' | 'column') => string,
}

export type BuildSqlHelper = {
	skip: (value: number) => string,
	take: (value: number) => string,
	setParam: (value: ParamType) => string,
	delimitedIdentifiers: (identifier: string) => string,
	aliasAs: (alias: string, type: 'table' | 'column') => string,
}

export abstract class SqlSegmentOwner {
	protected abstract getSegmentData: () => SqlSegmentData
	static getSegmentData(target: SqlSegmentOwner) {
		return target.getSegmentData()
	}
}

function parseSegment(value: SegmentValue, helper: BuildSqlHelper) {
	if (value === null) {
		return 'null'
	} else if (value instanceof Array) {
		if (value.length === 0) {
			return `(null)`
		} else {
			return `(${value.map((v) => parseSegment(v, helper) satisfies string).join(',')})`
		}
	} else if (value instanceof SqlSegmentLike) {
		return SqlSegmentLike.buildSqlStr(value, helper)
	} else if (typeof value === 'string') {
		return helper.setParam(value)
	} else if (typeof value === 'number') {
		return helper.setParam(value)
	} else if (typeof value === 'boolean') {
		return helper.setParam(value)
	} else if (value instanceof Date) {
		return helper.setParam(value)
	} else {
		throw new Error(`unknown value:${value satisfies never}`)
	}
}

export class SqlSegmentWrapper extends SqlSegmentOwner {
	constructor(
		private segmentData: SqlSegmentData
	) {
		super()
	}
	protected getSegmentData = () => {
		return this.segmentData
	}
}







export function sql(strings: TemplateStringsArray, ...values: SegmentValue[]) {
	return new SqlSegmentWrapper({ strings: Array.from(strings), values })
}

export class Column<N extends boolean = boolean, R = unknown> extends SqlSegmentOwner {
	protected getSegmentData = () => SqlSegmentOwner.getSegmentData(this.opts.sqlSegment)
	constructor(
		protected opts: {
			sqlSegment: SqlSegmentOwner,
			withNull: N,
			format: (raw: unknown) => Promise<R>,
		}
	) {
		super()
		this.opts = opts
	}


	withNull<const N extends boolean>(value: N) {
		return new Column<N, R>({
			sqlSegment: this.opts.sqlSegment,
			format: this.opts.format,
			withNull: value,
		})
	}

	format = <R2>(value: (value: R) => Async<R2>): Column<N, R2> => {
		return new Column<N, R2>({
			sqlSegment: this.opts.sqlSegment,
			format: async (raw) => value(await this.opts.format(raw)),
			withNull: this.opts.withNull,
		})
	}
}

export type SqlViewTemplate = DeepTemplate<Column<boolean, unknown>>

type _Relation<N extends boolean, VT extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }> = {
	[key in keyof VT]
	: VT[key] extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate } ? _Relation<N, VT[key]>
	: VT[key] extends Column<infer N2, infer R> ? Column<N2 & N, R>
	: never
}

export type Relation<N extends boolean, VT extends SqlViewTemplate> = N extends false ? VT
	: VT extends Column<infer N2, infer R> ? Column<N2 & N, R>
	: VT extends readonly SqlViewTemplate[] ? _Relation<N, VT>
	: VT extends { readonly [key: string]: SqlViewTemplate } ? _Relation<N, VT>
	: never


export class SelectSqlStruct {
	constructor(
		public opts: {
			from: Array<{
				alias: SqlSegmentOwner,
				expr: (helper: BuildSqlHelper) => SqlSegmentOwner,
			}>,
			join: Array<{
				type: 'left' | 'inner',
				lateral: boolean,
				alias: SqlSegmentOwner,
				expr: (helper: BuildSqlHelper) => SqlSegmentOwner,
				condation: (helper: BuildSqlHelper) => SqlSegmentOwner,
			}>,
			where: Array<(helper: BuildSqlHelper) => SqlSegmentOwner>,
			groupBy: Array<(helper: BuildSqlHelper) => SqlSegmentOwner>,
			having: Array<(helper: BuildSqlHelper) => SqlSegmentOwner>,
			order: Array<{
				order: 'asc' | 'desc',
				expr: (helper: BuildSqlHelper) => SqlSegmentOwner,
			}>,
			take: null | number,
			skip: number,
		}
	) { }

	state() {
		const state = new Set<SqlState>()
		if (this.opts.join.some((e) => e.type === 'inner')) { state.add('innerJoin') }
		if (this.opts.join.some((e) => e.type === 'left')) { state.add('leftJoin') }
		if (this.opts.where && this.opts.where.length > 0) { state.add('where') }
		if (this.opts.groupBy && this.opts.groupBy.length > 0) { state.add('groupBy') }
		if (this.opts.having && this.opts.having.length > 0) { state.add('having') }
		if (this.opts.order && this.opts.order.length > 0) { state.add('order') }
		if ((this.opts.skip ?? 0) > 0) { state.add('skip') }
		if (this.opts.take !== null) { state.add('take') }
		return state
	}

	private buildSqlBodySourceStr(helper: BuildSqlHelper) {
		if (this.opts.from.length === 0) { return '' }
		const buildResult: string[] = []
		this.opts.from.forEach((e) => {
			buildResult.push(`${SqlSegmentLike.buildSqlStr(e.expr, helper) satisfies string} ${SqlSegmentLike.buildSqlStr(e.alias, helper) satisfies string}`)
		})
		buildResult.push(`${this.opts.from.map((e) => `${SqlSegmentLike.buildSqlStr(e.expr, helper) satisfies string} ${SqlSegmentLike.buildSqlStr(e.alias, helper) satisfies string}`).join(',')}`)
		this.opts.join.forEach((join) => {
			buildResult.push([
				join.type,
				'join',
				join.lateral ? 'lateral' : '',
				SqlSegmentLike.buildSqlStr(join.expr, helper) satisfies string,
				SqlSegmentLike.buildSqlStr(join.alias, helper) satisfies string,
				exec(() => {
					const r = SqlSegmentLike.buildSqlStr(join.condation, helper)
					return r.length === 0 ? '' : `on ${r}`
				}) satisfies string,
			].filter((v) => v !== '').join(' '))
		})
		exec(() => {
			if (this.opts.where.length === 0) { return }
			buildResult.push(`where ${this.opts.where.map((e) => SqlSegmentLike.buildSqlStr(e, helper) satisfies string).join(' and ')}`)
		})
		exec(() => {
			if (this.opts.groupBy.length === 0) { return }
			buildResult.push(`group by ${this.opts.groupBy.map((e) => SqlSegmentLike.buildSqlStr(e, helper) satisfies string).join(',')}`)
		})
		exec(() => {
			if (this.opts.having.length === 0) { return }
			buildResult.push(`having ${this.opts.having.map((e) => SqlSegmentLike.buildSqlStr(e, helper) satisfies string).join(' and ')}`)
		})
		exec(() => {
			if (this.opts.order.length === 0) { return }
			buildResult.push(`order by ${this.opts.order.map((e) => `${SqlSegmentLike.buildSqlStr(e.expr, helper) satisfies string} ${e.order satisfies 'asc' | 'desc'} NULLS FIRST`).join(',')}`)
		})
		if (this.opts.skip) {
			buildResult.push(helper.skip(this.opts.skip).trim())
		}
		if (this.opts.take !== null) {
			buildResult.push(helper.take(this.opts.take).trim())
		}
		return buildResult.join(' ').trim()
	}
	buildSqlStr(helper: BuildSqlHelper, selectTarget: { expr: SqlSegment, alias: SqlSegment }[]) {
		const source = this.buildSqlBodySourceStr(helper) satisfies string
		return [
			`select ${buildSqlBodySelectStr(helper, selectTarget) satisfies string}`,
			!source ? '' : 'from',
			source,
		].filter((v) => v !== '').join(' ')
	}
	bracket(helper: BuildSqlHelper, opts: {
		selectTarget: { expr: SqlSegment, alias: SqlSegment }[],
		tableAlias: SqlSegment,
	}) {
		return new SelectSqlStruct({
			from: [
				{
					alias: opts.tableAlias,
					expr: (helper) => new SqlSegment({ strings: [`(${this.buildSqlStr(helper, opts.selectTarget) satisfies string})`], values: [] }),
				}
			],
			join: [],
			where: [],
			groupBy: [],
			having: [],
			order: [],
			take: null,
			skip: 0,
		})
	}
}

export type SqlState = 'leftJoin' | 'innerJoin' | 'where' | 'groupBy' | 'having' | 'take' | 'skip' | 'order'


export function buildSqlBodySelectStr(helper: BuildSqlHelper, selectTarget: { expr: SqlSegment, alias: SqlSegment }[]) {
	return [...selectTarget]
		.map(({ expr, alias }) => `${SqlSegmentLike.buildSqlStr(expr, helper) satisfies string} ${SqlSegmentLike.buildSqlStr(alias, helper) satisfies string}`)
		.join(',') ?? '1'
}


export const createColumn = (sqlWrapper: SqlSegment) => new Column<true, unknown>({
	sqlSegment: sqlWrapper,
	format: async (raw) => raw,
	withNull: true,
})

