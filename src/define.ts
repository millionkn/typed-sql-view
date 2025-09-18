import { exec } from "./tools.js"



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

export type RawSqlSegment = {
	strings: TemplateStringsArray,
	values: SegmentValue[],
}

export abstract class SqlSegmentLike {
	protected abstract asSegment: () => RawSqlSegment
	static asSegment(target: SqlSegmentLike) {
		return target.asSegment()
	}
}

export class SqlSegment extends SqlSegmentLike {
	protected asSegment: () => RawSqlSegment
	constructor(
		opts: RawSqlSegment
	) {
		super()
		this.asSegment = () => opts
	}
}



type SegmentValue =
	| Array<SegmentValue>
	| string | number | boolean | null | Date
	| SqlSegmentLike

function parseSegment(value: SegmentValue, buildCtx: BuildTools): string {
	if (value === null) {
		return 'null'
	} else if (value instanceof Array) {
		if (value.length === 0) {
			return `(null)`
		} else {
			return `(${value.map((v) => parseSegment(v, buildCtx)).join(',')})`
		}
	} else if (value instanceof SqlSegmentLike) {
		return value[sym2].parseSegment(buildCtx)
	} else if (typeof value === 'string') {
		return buildCtx.setParam(value)
	} else if (typeof value === 'number') {
		return buildCtx.setParam(value)
	} else if (typeof value === 'boolean') {
		return buildCtx.setParam(value)
	} else if (value instanceof Date) {
		return buildCtx.setParam(value)
	} else {
		throw new Error(`unknown value:${value satisfies never}`)
	}
}

export function sql(strings: TemplateStringsArray, ...values: SegmentValue[]) {
	return new SqlSegment({ strings, values })
}

export class Column<N extends boolean = boolean, R = unknown> extends SqlSegmentLike {

	static create(sqlSegment: SqlSegment) {

		SqlSegmentLike.asSegment(sqlSegment)

		return new Column<true, unknown>(sqlSegment[sym2](), {
			format: async (raw) => raw,
			withNull: true,
		})
	}
	[sym]: {
		withNull: N,
		format: (raw: unknown) => Promise<R>,
	}
	private constructor(
		rawSqlSegment: RawSqlSegment,
		opts: {
			withNull: N,
			format: (raw: unknown) => Promise<R>,
		}
	) {
		super()
		this[sym2] = () => rawSqlSegment
		this[sym] = opts
	}


	withNull<const N extends boolean>(value: N) {
		return new Column<N, R>(this[sym2](), {
			...this[sym],
			withNull: value,
		})
	}

	format = <R2>(value: (value: R) => Async<R2>): Column<N, R2> => {
		const format = this[sym].format
		return new Column<N, R2>(this[sym2](), {
			...this[sym],
			format: async (raw) => value(await format(raw))
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



export type Adapter = {
	skip: (value: number) => string,
	take: (value: number) => string,
	paramHolder: (index: number) => string,
	delimitedIdentifiers: (identifier: string) => string,
	aliasAs: (alias: string, type: 'table' | 'column') => string,
}

export type BuildTools = {
	genAlias: () => SqlSegment,
	setParam: (value: unknown) => string,
	createHolder: () => {
		expr: string,
		replaceWith: (expr: string) => void,
	},
}



export class SelectSqlStruct {
	constructor(
		public opts: {
			from: {
				alias: () => string,
				expr: RawSqlSegment,
			}[],
			join: {
				type: 'left' | 'inner',
				lateral: boolean,
				alias: RawSqlSegment,
				expr: RawSqlSegment,
				condation: RawSqlSegment,
			}[],
			where: Array<RawSqlSegment>,
			groupBy: Array<RawSqlSegment>,
			having: Array<RawSqlSegment>,
			order: {
				order: 'asc' | 'desc',
				expr: RawSqlSegment,
			}[],
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

	private buildSqlBodySourceStr(adapter: Adapter) {
		if (this.opts.from.length === 0) { return '' }
		const buildResult: string[] = []
		buildResult.push(`${this.opts.from.map((e) => `${e.expr} ${e.alias()}`).join(',')}`)
		this.opts.join.forEach((join) => {
			buildResult.push([
				join.type,
				'join',
				join.lateral ? 'lateral' : '',
				join.expr,
				join.alias,
				join.condation.length === 0 ? '' : `on ${join.condation}`,
			].filter((v) => v !== '').join(' '))
		})
		exec(() => {
			if (this.opts.where.length === 0) { return }
			buildResult.push(`where ${this.opts.where.join(' and ')}`)
		})
		exec(() => {
			if (this.opts.groupBy.length === 0) { return }
			buildResult.push(`group by ${this.opts.groupBy.join(',')}`)
		})
		exec(() => {
			if (this.opts.having.length === 0) { return }
			buildResult.push(`having ${this.opts.having.join(' and ')}`)
		})
		exec(() => {
			if (this.opts.order.length === 0) { return }
			buildResult.push(`order by ${this.opts.order.map((e) => `${e.expr} ${e.order} NULLS FIRST`).join(',')}`)
		})
		if (this.opts.skip) {
			buildResult.push(adapter.skip(this.opts.skip).trim())
		}
		if (this.opts.take !== null) {
			buildResult.push(adapter.take(this.opts.take).trim())
		}
		return buildResult.join(' ').trim()
	}
	buildSqlStr(adapter: Adapter, selectTarget: { expr: string, alias: string }[]) {
		const source = this.buildSqlBodySourceStr(adapter)
		return [
			`select ${buildSqlBodySelectStr(adapter, selectTarget)}`,
			!source ? '' : 'from',
			source,
		].filter((v) => v !== '').join(' ')
	}
	bracket(opts: {
		selectTarget: { expr: string, alias: string }[],
		tableAlias: string,
	}) {
		return new SelectSqlStruct({
			from: [
				{
					alias: opts.tableAlias,
					expr: `(${this.buildSqlStr(opts.selectTarget)})`,
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


export function buildSqlBodySelectStr(adapter: Adapter, selectTarget: { expr: string, alias: string }[]) {
	return [...selectTarget]
		.map(({ expr, alias }) => `${expr} ${adapter.aliasAs(alias, 'column')}`)
		.join(',') ?? '1'
}




export const createResolver = exec(() => {
	let _nsIndex = 0
	return <T>() => {
		const nsIndex = _nsIndex++
		let index = 0
		const saved = new Map<string, () => T>()
		return {
			createHolder: (getValue: () => T) => {
				const key = `holder_${nsIndex}_${index++}`
				saved.set(key, getValue)
				return `''""''""${key}''""''""`
			},
			resolve: (str: string, unResolved: (key: string) => string): Array<string | (() => T)> => {
				return str.split(`''""''""`).map((str, i) => {
					if (i % 2 === 0) { return str }
					const getValue = saved.get(str)
					if (getValue) {
						return getValue
					} else {
						return unResolved(str)
					}
				})
			}
		}
	}
})
export const createColumn = (sqlWrapper: SqlSegment) => Column.create(sqlWrapper)
export type Async<T> = T | PromiseLike<T>
