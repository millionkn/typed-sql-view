import { Async, connectWith, DeepTemplate, exec } from "./tools.js"

type _ParamType = string | number | boolean | null | Date
export type ParamType = _ParamType | Array<_ParamType>

export const sym = Symbol()

export abstract class Segment {
	abstract [sym]: {
		type: string,
		createBuilderCtx: () => BuilderCtx,
	}
}

export type SyntaxAdapter = {
	pagination: (skip: number, take: number | null) => string,
	tableAlias: (alias: string) => string,
	selectAndAlias: (select: string, alias: string) => string,
	columnRef: (tableAlias: string, columnAlias: string) => string,
	order: (items: {
		expr: string,
		order: 'ASC' | 'DESC',
		nulls: 'FIRST' | 'LAST',
	}[]) => string,
}

export type BuildSqlHelper = {
	adapter: SyntaxAdapter,
	setParam: (value: ParamType) => string,
	fetchColumnAlias: (key: symbol | string) => string,
	fetchTableAlias: (key: symbol | string) => string,
}

export class Holder {
	constructor(
		public readonly effectOn: (helper: BuildSqlHelper) => string
	) { }
}

export type ActiveExpr = string | Holder

export type BuilderCtx = {
	emitInnerUsed: () => void,
	buildExpr: () => ActiveExpr[],
}

export class InnerSegment extends Segment {
	[sym] = {
		type: 'innerSegment' as const,
		createBuilderCtx: () => this.createBuilderCtx(),
	}
	constructor(
		private createBuilderCtx: () => BuilderCtx,
	) {
		super()
	}
}

export const sql = (strings: TemplateStringsArray, ...values: Array<
	| ParamType
	| Segment
>): Segment => {
	if (strings.length === 0) {
		throw new Error('sql strings length is 0')
	}
	if (strings.length === 1 && strings[0].trim() === '') {
		throw new Error('sql strings is empty')
	}
	return new InnerSegment(() => {
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
			if (value instanceof Segment) {
				return value[sym].createBuilderCtx()
			} else {
				const holder = new Holder((helper) => helper.setParam(value satisfies ParamType))
				return {
					emitInnerUsed: () => { },
					buildExpr: () => [holder],
				}
			}
		}).filter((e) => e !== null)
		return {
			emitInnerUsed: () => arr.forEach((e) => e.emitInnerUsed()),
			buildExpr: () => arr.map((e) => e.buildExpr()).flat(1),
		}
	})
}
export const rawSql = (sql: string): Segment => {
	if (sql.length === 0) {
		throw new Error('sql length is 0')
	}
	return new InnerSegment(() => {
		return {
			emitInnerUsed: () => { },
			buildExpr: () => [sql],
		}
	})
}

export class ColumnRef<N extends boolean = boolean, R = unknown> extends Segment {
	[sym] = {
		type: 'column' as const,
		createBuilderCtx: (): BuilderCtx => {
			return this.opts.builderCtx
		},
	}
	constructor(
		private opts: {
			builderCtx: BuilderCtx,
			withNull: N,
			format: (raw: unknown) => Promise<R>,
		}
	) {
		super()
	}
	static getOpts(column: ColumnRef<boolean, unknown>) {
		return column.opts
	}
	withNull<const N extends boolean>(value: N) {
		return new ColumnRef<N, R>({
			builderCtx: this.opts.builderCtx,
			format: this.opts.format,
			withNull: value,
		})
	}

	format = <R2>(value: (value: R) => Async<R2>): ColumnRef<N, R2> => {
		return new ColumnRef<N, R2>({
			builderCtx: this.opts.builderCtx,
			format: async (raw) => value(await this.opts.format(raw)),
			withNull: this.opts.withNull,
		})
	}
}

export type SqlViewTemplate = DeepTemplate<ColumnRef>

type _Relation<N extends boolean, VT extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }> = {
	[key in keyof VT]
	: VT[key] extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate } ? _Relation<N, VT[key]>
	: VT[key] extends ColumnRef<infer N2, infer R> ? ColumnRef<(N | N2) extends false ? false : (N | N2), R>
	: never
}

export type Relation<N extends boolean, VT extends SqlViewTemplate> = N extends false ? VT
	: VT extends ColumnRef<infer N2, infer R> ? ColumnRef<N | N2, R>
	: VT extends readonly SqlViewTemplate[] ? _Relation<N, VT>
	: VT extends { readonly [key: string]: SqlViewTemplate } ? _Relation<N, VT>
	: never

type _AssertWithNull<N extends boolean, VT extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }> = {
	[key in keyof VT]
	: VT[key] extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate } ? _AssertWithNull<N, VT[key]>
	: VT[key] extends ColumnRef<infer N2, infer R> ? ColumnRef<N, R>
	: never
}

export type AssertWithNull<N extends boolean, VT extends SqlViewTemplate> =
	VT extends ColumnRef<infer N2, infer R> ? ColumnRef<N, R>
	: VT extends readonly SqlViewTemplate[] ? _AssertWithNull<N, VT>
	: VT extends { readonly [key: string]: SqlViewTemplate } ? _AssertWithNull<N, VT>
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
				order: 'ASC' | 'DESC',
				nulls: 'FIRST' | 'LAST',
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
					this.opts.from.alias.effectOn(helper),
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
				new Holder((helper) => helper.adapter.tableAlias(join.alias.effectOn(helper))),
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
			buildResult.push(new Holder((helper) => helper.adapter.order(this.opts.order.map((item) => {
				return {
					expr: parseAndEffectOnHelper(item.expr, helper),
					order: item.order,
					nulls: item.nulls,
				}
			}))))
		})
		buildResult.push(new Holder((helper) => helper.adapter.pagination(this.opts.skip, this.opts.take)))
		return buildResult.flat(1)
	}
	bracket(tableAlias: Holder, selectTarget: Map<Holder, { buildExpr: () => ActiveExpr[] }>) {
		return new SelectBodyStruct({
			from: {
				expr: [
					`(`,
					parseFullSelectExpr({
						selectTarget,
						struct: this,
					}),
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

export function parseFullSelectExpr(opts: {
	selectTarget: Map<Holder, { buildExpr: () => ActiveExpr[] }>,
	struct: SelectBodyStruct,
}): ActiveExpr[] {
	const selectPart = opts.selectTarget.size === 0 ? '1' : connectWith([...opts.selectTarget.entries()].map(([holder, { buildExpr }]) => {
		return new Holder((helper) => {
			const selectStr = parseAndEffectOnHelper(buildExpr(), helper)
			const aliasStr = parseAndEffectOnHelper([holder], helper)
			return helper.adapter.selectAndAlias(selectStr, aliasStr)
		})
	}), () => ',')
	return [
		`select `,
		selectPart,
		' from ',
		opts.struct.buildBodyExpr(),
	].flat(1)
}

export type SqlState = 'leftJoin' | 'innerJoin' | 'where' | 'groupBy' | 'having' | 'take' | 'skip' | 'order'

export function parseAndEffectOnHelper(expr: ActiveExpr[], helper: BuildSqlHelper): string {
	return expr.map((e): string => typeof e === 'string' ? e : e.effectOn(helper)).join('')
}

export type SqlExecuteBundle<R> = {
	sql: string;
	paramArr: unknown[];
	formatter: (arr: { [key: string]: unknown }[]) => Promise<R>;
};