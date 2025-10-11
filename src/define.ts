import { SqlView } from "./sqlView.js"
import { Async, connectWith, DeepTemplate, exec, iterateTemplate } from "./tools.js"

export const sym = Symbol()



type _ParamType = string | number | boolean | null | Date
export type ParamType = _ParamType | Array<_ParamType>

export const innerTypeSym = Symbol()

export abstract class InnerClass {
	abstract [innerTypeSym]: unknown
}

export type SyntaxAdapter = {
	skip: (value: number) => string,
	take: (value: number) => string,
	selectAndAlias: (select: string, alias: string) => string,
}

export type BuildSqlHelper = {
	adapter: SyntaxAdapter,
	setParam: (value: ParamType) => string,
	fetchColumnAlias: (key: object) => string,
	fetchTableAlias: (key: object) => string,
}

export class Holder extends InnerClass {
	[innerTypeSym] = 'holder' as const
	[sym]: {
		parse: (helper: BuildSqlHelper) => string
	}
	constructor(
		parse: (helper: BuildSqlHelper) => string
	) {
		super()
		this[sym] = { parse }
	}
}

export type ActiveExpr = string | Holder

export type BuilderCtx = {
	emitInnerUsed: () => void,
	buildExpr: () => ActiveExpr[],
}

export class Segment extends InnerClass {
	[innerTypeSym] = 'segment' as const
	constructor(
		public readonly createBuilderCtx: () => BuilderCtx,
	) {
		super()
	}
}

export const sql = (strings: TemplateStringsArray, ...values: Array<
	| ParamType
	| Segment
	| Holder
	| Column
	| SqlView<SqlViewTemplate>
>) => new Segment(() => {
	const arr = connectWith(strings.map((str): BuilderCtx => {
		return {
			emitInnerUsed: () => { },
			buildExpr: () => [str],
		}
	}), (index): BuilderCtx => {
		const value = values[index]
		if (!(value instanceof InnerClass)) {
			return {
				emitInnerUsed: () => { },
				buildExpr: () => [new Holder((helper) => helper.setParam(value))],
			}
		} else if (value[innerTypeSym] === 'segment') {
			return value.createBuilderCtx()
		} else if (value[innerTypeSym] === 'holder') {
			return {
				emitInnerUsed: () => { },
				buildExpr: () => [value],
			}
		} else if (value[innerTypeSym] === 'column') {
			return Column.getOpts(value).builderCtx
		} else if (value[innerTypeSym] === 'sqlView') {
			const builder = value._createStructBuilder()
			return {
				emitInnerUsed: () => {
					iterateTemplate(builder.template, (c) => c instanceof Column, (c) => Column.getOpts(c).builderCtx.emitInnerUsed())
					builder.emitInnerUsed()
				},
				buildExpr: () => {
					const struct = builder.buildBody({ order: true })
					return [
						`select `,
						getSelectAliasExpr(new Map()),
						' ',
						struct.getBodyExpr(),
					].flat(1)
				},
			}
		} else {
			return value satisfies never
		}
	})
	return {
		emitInnerUsed: () => arr.forEach((e) => e.emitInnerUsed()),
		buildExpr: () => arr.map((e) => e.buildExpr()).flat(1),
	}
})

export class Column<N extends boolean = boolean, R = unknown> extends InnerClass {
	[innerTypeSym] = 'column' as const
	constructor(
		private opts: {
			builderCtx: BuilderCtx,
			withNull: N,
			format: (raw: unknown) => Promise<R>,
		}
	) {
		super()
	}
	static getOpts(column: Column<boolean, unknown>) {
		return column.opts
	}
	withNull<const N extends boolean>(value: N) {
		return new Column<N, R>({
			builderCtx: this.opts.builderCtx,
			format: this.opts.format,
			withNull: value,
		})
	}

	format = <R2>(value: (value: R) => Async<R2>): Column<N, R2> => {
		return new Column<N, R2>({
			builderCtx: this.opts.builderCtx,
			format: async (raw) => value(await this.opts.format(raw)),
			withNull: this.opts.withNull,
		})
	}
}

export type SqlViewTemplate = DeepTemplate<Column>

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





export class SelectBodyStruct {
	constructor(
		public opts: {
			from: {
				expr: ActiveExpr[],
				alias: Holder,
			},
			join: Array<{
				type: 'left' | 'inner',
				lateral: boolean,
				expr: ActiveExpr[],
				condation: ActiveExpr[],
				alias: Holder,
			}>,
			where: Array<{
				expr: ActiveExpr[],
			}>,
			groupBy: Array<{
				expr: ActiveExpr[],
			}>,
			having: Array<{
				expr: ActiveExpr[],
			}>,
			order: Array<{
				order: 'asc' | 'desc',
				expr: ActiveExpr[],
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

	getBodyExpr(): ActiveExpr[] {
		const buildResult: ActiveExpr[] = []
		// todo
		// exec(() => {
		// 	buildResult.push(expr`${this.opts.from.expr}`[
		// 		...,
		// 		' ',
		// 		(helper) => helper.adapter.aliasAs(this.opts.from.alias(helper), 'table')
		// 	])
		// })
		// this.opts.join.forEach((join) => {
		// 	buildResult.push([
		// 		join.type satisfies 'left' | 'inner',
		// 		'join', join.lateral ? ' lateral' : '',
		// 		...join.expr,
		// 		(helper) => helper.adapter.aliasAs(join.alias(helper), 'column'),
		// 		' on ',
		// 		join.condation,
		// 	])
		// })
		// exec(() => {
		// 	const where = this.opts.where.map((e) => e.expr).filter((e) => e.length > 0)
		// 	if (where.length === 0) { return }
		// 	where.forEach((expr, i) => {
		// 		if (i === 0) {
		// 			buildResult.push([`where `, expr])
		// 		} else {
		// 			buildResult.push(['and ', expr])
		// 		}
		// 	})
		// })
		// exec(() => {
		// 	if (this.opts.groupBy.length === 0) { return }
		// 	this.opts.groupBy.forEach(({ expr }, i) => {
		// 		if (i === 0) {
		// 			buildResult.push([`group by `, expr])
		// 		} else {
		// 			buildResult.push(', ', expr)
		// 		}
		// 	})
		// })
		// exec(() => {
		// 	if (this.opts.having.length === 0) { return }
		// 	this.opts.having.forEach(({ expr }, i) => {
		// 		if (i === 0) {
		// 			buildResult.push(`having `, expr)
		// 		} else {
		// 			buildResult.push(' and ', expr)
		// 		}
		// 	})
		// })
		// exec(() => {
		// 	if (this.opts.order.length === 0) { return }
		// 	this.opts.order.forEach(({ expr, order }, i) => {
		// 		if (i === 0) {
		// 			buildResult.push(`order by `, expr, ` ${order} NULLS FIRST`)
		// 		} else {
		// 			buildResult.push(', ', expr, ` ${order} NULLS FIRST`)
		// 		}
		// 	})
		// })
		// if (this.opts.skip) {
		// 	const skip = this.opts.skip
		// 	buildResult.push(new Holder((helper) => helper.adapter.skip(skip).trim()))
		// }
		// if (this.opts.take !== null) {
		// 	const take = this.opts.take
		// 	buildResult.push(new Holder((helper) => helper.adapter.take(take).trim()))
		// }
		return buildResult
	}
	bracket(selectTarget: Array<{ expr: ActiveExpr[], alias: Holder }>) {
		const bodyExpr = this.getBodyExpr()
		// todo
		return new SelectBodyStruct({
			from: {
				expr: [
					// `(`,
					// `select`,
					// buildSqlBodySelectExpr(opts.selectTarget),
					// !opts.selectTarget.length ? '' : 'from',
					// bodyExpr,
					// `)`
				],
				alias: exec(() => {
					const key = {}
					return new Holder((helper) => helper.fetchTableAlias(key))
				}),
			},
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

export function getSelectAliasExpr(selectTarget: Map<Holder, ActiveExpr[]>): ActiveExpr[] {
	if (selectTarget.size === 0) { return ['1'] }
	return [...selectTarget.entries()].map(([holder, expr]) => {
		return new Holder((helper) => {
			const selectStr = parseExpr(expr, helper)
			const aliasStr = parseExpr([holder], helper)
			return helper.adapter.selectAndAlias(selectStr, aliasStr)
		})
	})
}

export type SqlState = 'leftJoin' | 'innerJoin' | 'where' | 'groupBy' | 'having' | 'take' | 'skip' | 'order'


export function createExprTools() {
	const spliter = (label: string) => `'"\`'"\`${label}'"\`'"\``
	const helper: BuildSqlHelper = {
		fetchColumnAlias: exec(() => {
			const resultMapper = new Map<object, string>()
			return (key) => {
				if (!resultMapper.has(key)) {
					const result = `${spliter('columnAlias')}${resultMapper.size}${spliter('columnAlias')}`
					resultMapper.set(key, result)
				}
				return resultMapper.get(key)!
			}
		}),
		fetchTableAlias: exec(() => {
			const resultMapper = new Map<object, string>()
			return (key) => {
				if (!resultMapper.has(key)) {
					const result = `${spliter('tableAlias')}${resultMapper.size}${spliter('tableAlias')}`
					resultMapper.set(key, result)
				}
				return resultMapper.get(key)!
			}
		}),
		setParam: exec(() => {
			let index = 0
			return () => {
				const result = `${spliter('param')}${index++}${spliter('param')}`
				return result
			}
		}),
		adapter: {
			skip: (value) => `${spliter('skip')}${value satisfies number}${spliter('skip')}`,
			take: (value) => `${spliter('take')}${value satisfies number}${spliter('take')}`,
			selectAndAlias: (select, alias) => `${spliter('select')}${select.trim()}${spliter('select')} as ${spliter('userAlias')}${alias.trim()}${spliter('userAlias')}`,
		},
	}
	const resultMapper = new Map<string, object>()
	return {
		fetchExprKey: (expr: ActiveExpr[]): object => {
			const key = parseExpr(expr, helper).trim()
			if (!resultMapper.has(key)) {
				const result = {}
				resultMapper.set(key, result)
			}
			return resultMapper.get(key)!
		},
	}
}

export function parseExpr(expr: ActiveExpr[], helper: BuildSqlHelper): string {
	return expr.map((e): string => typeof e === 'string' ? e : e[sym].parse(helper)).join('')
}

export function buildSqlBodySelectExpr(selectTarget: {
	expr: ActiveExpr[],
	alias: Holder,
}[]): ActiveExpr[] {
	if (selectTarget.length === 0) {
		return ['1']
	}
	return selectTarget.map(({ expr, alias }) => {
		return new Holder((helper) => {
			const selectStr = parseExpr(expr, helper)
			const aliasStr = parseExpr([alias], helper)
			return helper.adapter.selectAndAlias(selectStr, aliasStr)
		})
	})
}