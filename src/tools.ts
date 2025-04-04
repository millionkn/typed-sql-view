export const sym = Symbol()

export const exec = <T>(fun: () => T): T => fun()

export type DeepTemplate<I> = I | (readonly [...DeepTemplate<I>[]]) | { readonly [key: string]: DeepTemplate<I> }

export const iterateTemplate = (template: SqlViewTemplate, cb: (column: Column<string>) => unknown): unknown => {
	if (template instanceof Column) {
		return cb(template)
	} else if (template instanceof Array) {
		return template.map((t) => iterateTemplate(t, cb))
	} else {
		return Object.fromEntries(Object.entries(template).map(([key, t]) => [key, iterateTemplate(t, cb)]))
	}
}
export class Column<T extends string, N extends boolean = boolean, R = unknown> {
	static create(expr: string) {
		return new Column({
			expr,
			assert: '',
			format: async (raw) => raw,
			withNull: true,
		})
	}
	[sym]: {
		expr: string,
		withNull: N,
		format: (raw: unknown) => Promise<R>,
		assert: T,
	}
	private constructor(
		opts: {
			expr: string,
			withNull: N,
			format: (raw: unknown) => Promise<R>,
			assert: T,
		}
	) {
		this[sym] = opts
	}


	withNull<const N extends boolean>(value: N) {
		return new Column<T, N, R>({
			...this[sym],
			withNull: value,
		})
	}

	format<R2>(value: (value: R) => Async<R2>) {
		const format = this[sym].format
		return new Column<T, N, R2>({
			...this[sym],
			format: async (raw) => value(await format(raw))
		})
	}

	assert<T2 extends string>(pre: T, cur: T2) {
		if (this[sym].assert !== pre) {
			throw new Error(`assert tag '${pre}',but saved is '${this[sym].assert}'`)
		}
		return new Column<T2, N, R>({
			...this[sym],
			assert: cur,
		})
	}

	toString() {
		return this[sym].expr
	}
}

export type SqlViewTemplate = DeepTemplate<Column<string, boolean, unknown>>

type _Relation<N extends boolean, VT extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }> = {
	[key in keyof VT]
	: VT[key] extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate } ? _Relation<N, VT[key]>
	: VT[key] extends Column<infer T, infer N2, infer R> ? Column<T, (N2 & N) extends true ? true : boolean, R>
	: never
}

export type Relation<N extends boolean, VT extends SqlViewTemplate> = N extends false ? VT
	: VT extends Column<infer T, infer N2, infer R> ? Column<T, (N2 & N) extends true ? true : boolean, R>
	: VT extends readonly SqlViewTemplate[] ? _Relation<N, VT>
	: VT extends { readonly [key: string]: SqlViewTemplate } ? _Relation<N, VT>
	: never



export type Adapter = {
	skip: (value: number) => string,
	take: (value: number) => string,
	paramHolder: (index: number) => string,
}

export type BuildCtx = {
	genAlias: () => string,
	setParam: (value: unknown) => string,
	createHolder: () => {
		expr: string,
		replaceWith: (expr: string) => void,
	},
}

export function hasOneOf<T>(items: Iterable<T>, arr: NoInfer<T>[]) {
	return !![...items].find((e) => arr.includes(e))
}

export class SqlBody {
	constructor(
		public opts: {
			from: {
				alias: string,
				expr: string,
			}[],
			join: {
				type: 'left' | 'inner',
				alias: string,
				expr: string,
				condation: string,
			}[],
			where: Array<string>,
			groupBy: Array<string>,
			having: Array<string>,
			order: {
				order: 'asc' | 'desc',
				expr: string,
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

	buildSqlBodySourceStr() {
		if (this.opts.from.length === 0) { return '' }
		const buildResult: string[] = []
		buildResult.push(`${this.opts.from.map((e) => `${e.expr} ${e.alias}`).join(',')}`)
		this.opts.join.forEach((join) => {
			const condation = join.condation
			buildResult.push(`${join.type} join ${join.expr} ${join.alias} ${condation.length === 0 ? '' : `on ${condation}`}`)
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
			buildResult.push(`order by ${this.opts.order.map((e) => `${e.expr} ${e.order}`).join(',')}`)
		})
		if (this.opts.skip) {
			buildResult.push(`''""skip__${this.opts.skip}''""`)
		}
		if (this.opts.take !== null) {
			buildResult.push(`''""take__${this.opts.take}''""`)
		}
		return buildResult.join(' ').trim()
	}
	buildSqlStr(selectTarget: { expr: string, alias: string }[]) {
		const source = this.buildSqlBodySourceStr()
		return [
			`select ${buildSqlBodySelectStr(selectTarget)}`,
			!source ? '' : 'from',
			source,
		].join(' ')
	}
	bracket(opts: {
		selectTarget: { expr: string, alias: string }[],
		tableAlias: string,
	}) {
		return new SqlBody({
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


export function buildSqlBodySelectStr(selectTarget: { expr: string, alias: string }[]) {
	return [...selectTarget]
		.map(({ expr, alias }) => `${expr} ${alias}`)
		.join(',') ?? '1'
}

export function pickConfig<K extends string, R>(key: K, config: { [key in K]: () => R }): R {
	return config[key]()
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
				return `''""${key}''""`
			},
			resolve: (str: string, unResolved: (key: string) => string): Array<string | (() => T)> => {
				return str.split(`''""`).map((str, i) => {
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
export const createColumn = (expr: string) => Column.create(expr)
export type Async<T> = T | PromiseLike<T>
