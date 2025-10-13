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
	tableAlias: (alias: string) => string,
	selectAndAlias: (select: string, alias: string) => string,
	columnRef: (tableAlias: string, columnAlias: string) => string,
}

export type BuildSqlHelper = {
	adapter: SyntaxAdapter,
	setParam: (value: ParamType) => string,
	fetchColumnAlias: (key: symbol | string) => string,
	fetchTableAlias: (key: symbol | string) => string,
}

export class Holder extends InnerClass {
	[innerTypeSym] = 'holder' as const
	[sym]: {
		effectOn: (helper: BuildSqlHelper) => string
	}
	constructor(
		parse: (helper: BuildSqlHelper) => string
	) {
		super()
		this[sym] = { effectOn: parse }
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
	| Column
	| SqlView<SqlViewTemplate>
>) => {
	if (strings.length === 0) {
		throw new Error('sql strings length is 0')
	}
	if (strings.length === 1 && strings[0].trim() === '') {
		throw new Error('sql strings is empty')
	}
	return new Segment(() => {
		const arr = connectWith(strings.map((str, index): BuilderCtx | null => {
			if (index === 0) { str = str.trimStart() }
			if (index === strings.length - 1) { str = str.trimEnd() }
			if (str === '') { return null }
			return {
				emitInnerUsed: () => { },
				buildExpr: () => [str],
			}
		}), (index): BuilderCtx => {
			const value = values[index]
			if (!(value instanceof InnerClass)) {
				const holder = new Holder((helper) => helper.setParam(value))
				return {
					emitInnerUsed: () => { },
					buildExpr: () => [holder],
				}
			} else if (value[innerTypeSym] === 'segment') {
				return value.createBuilderCtx()
			} else if (value[innerTypeSym] === 'column') {
				return Column.getOpts(value).builderCtx
			} else if (value[innerTypeSym] === 'sqlView') {
				const selectMapper: Map<Holder, {
					buildExpr: () => ActiveExpr[],
				}> = new Map()
				const builder = value[sym]()._createStructBuilder()
				return {
					emitInnerUsed: () => {
						iterateTemplate(builder.template, (c) => c instanceof Column, (c) => {
							const builderCtx = Column.getOpts(c).builderCtx
							builderCtx.emitInnerUsed()
							const key = Symbol('columnAlias')
							selectMapper.set(new Holder((helper) => helper.fetchColumnAlias(key)), {
								buildExpr: () => builderCtx.buildExpr(),
							})
						})
						builder.emitInnerUsed()
					},
					buildExpr: () => {
						const struct = builder.finalize({ order: true })
						return [
							`select `,
							getSelectAliasExpr(selectMapper),
							' from ',
							struct.buildBodyExpr(),
						].flat(1)
					},
				}
			} else {
				return value satisfies never
			}
		}).filter((e) => e !== null)
		return {
			emitInnerUsed: () => arr.forEach((e) => e.emitInnerUsed()),
			buildExpr: () => arr.map((e) => e.buildExpr()).flat(1),
		}
	})
}

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
		public readonly opts: {
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

	buildBodyExpr(): ActiveExpr[] {
		const buildResult: Array<ActiveExpr | ActiveExpr[]> = []
		exec(() => {
			buildResult.push(
				this.opts.from.expr,
				' ',
				new Holder((helper) => helper.adapter.tableAlias(
					this.opts.from.alias[sym].effectOn(helper),
				))
			)
		})
		this.opts.join.forEach((join) => {
			buildResult.push(
				join.type satisfies 'left' | 'inner',
				' join ',
				join.lateral ? 'lateral ' : '',
				join.expr,
				' ',
				new Holder((helper) => helper.adapter.tableAlias(join.alias[sym].effectOn(helper))),
			)
			if (join.condation.length > 0) {
				buildResult.push(' on ', join.condation)
			}
		})
		exec(() => {
			const where = this.opts.where.map((e) => e.expr).filter((e) => e.length > 0)
			if (where.length === 0) { return }
			where.forEach((expr, i) => {
				if (i === 0) {
					buildResult.push(` where `, expr)
				} else {
					buildResult.push(' and ', expr)
				}
			})
		})
		exec(() => {
			if (this.opts.groupBy.length === 0) { return }
			this.opts.groupBy.forEach(({ expr }, i) => {
				if (i === 0) {
					buildResult.push(` group by `, expr)
				} else {
					buildResult.push(', ', expr)
				}
			})
		})
		exec(() => {
			if (this.opts.having.length === 0) { return }
			this.opts.having.forEach(({ expr }, i) => {
				if (i === 0) {
					buildResult.push(` having `, expr)
				} else {
					buildResult.push(' and ', expr)
				}
			})
		})
		exec(() => {
			if (this.opts.order.length === 0) { return }
			this.opts.order.forEach(({ expr, order }, i) => {
				if (i === 0) {
					buildResult.push(` order by `, expr, ` ${order} NULLS FIRST`)
				} else {
					buildResult.push(', ', expr, ` ${order} NULLS FIRST`)
				}
			})
		})
		if (this.opts.skip) {
			const skip = this.opts.skip
			buildResult.push(' ', new Holder((helper) => helper.adapter.skip(skip).trim()))
		}
		if (this.opts.take !== null) {
			const take = this.opts.take
			buildResult.push(' ', new Holder((helper) => helper.adapter.take(take).trim()))
		}
		return buildResult.flat(1)
	}
	bracket(tableAlias: Holder, selectTarget: Map<Holder, { buildExpr: () => ActiveExpr[] }>) {
		const bodyExpr = this.buildBodyExpr()
		return new SelectBodyStruct({
			from: {
				expr: [
					`(`,
					`select `,
					getSelectAliasExpr(selectTarget),
					' from ',
					bodyExpr,
					`)`
				].flat(1),
				alias: tableAlias,
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

export function getSelectAliasExpr(selectTarget: Map<Holder, { buildExpr: () => ActiveExpr[] }>): ActiveExpr[] {
	if (selectTarget.size === 0) { return ['1'] }
	return [...selectTarget.entries()].map(([holder, { buildExpr }]) => {
		return new Holder((helper) => {
			const selectStr = parseAndEffectOnHelper(buildExpr(), helper)
			const aliasStr = parseAndEffectOnHelper([holder], helper)
			return helper.adapter.selectAndAlias(selectStr, aliasStr)
		})
	})
}

export type SqlState = 'leftJoin' | 'innerJoin' | 'where' | 'groupBy' | 'having' | 'take' | 'skip' | 'order'

export function parseAndEffectOnHelper(expr: ActiveExpr[], helper: BuildSqlHelper): string {
	return expr.map((e): string => typeof e === 'string' ? e : e[sym].effectOn(helper)).join('')
}