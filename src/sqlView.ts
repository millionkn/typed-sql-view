import { Column, Relation, SelectBodyStruct, SqlViewTemplate, BuildSqlHelper, ActiveExpr, InnerClass, innerTypeSym, Segment, Holder, createExprTools, getSelectAliasExpr } from "./define.js"
import { exec, hasOneOf, iterateTemplate, pickConfig } from "./tools.js"

export type BuildFlag = {
	order: boolean,
}


export type SelectStructBuilder<VT extends SqlViewTemplate> = {
	template: VT,
	emitInnerUsed: () => void,
	buildBody: (flag: BuildFlag) => SelectBodyStruct,
}

export type SelectResult<VT extends SqlViewTemplate> = VT extends readonly [] ? []
	: VT extends readonly [infer X extends SqlViewTemplate, ...infer Arr extends readonly SqlViewTemplate[]]
	? [SelectResult<X>, ...SelectResult<Arr>]
	: VT extends readonly (infer X extends SqlViewTemplate)[]
	? SelectResult<X>[]
	: VT extends Column<infer N, infer R>
	? (true extends N ? null : never) | R
	: VT extends { [key: string]: SqlViewTemplate }
	? { -readonly [key in keyof VT]: SelectResult<VT[key]> }
	: never

export const proxyBuilder = <VT extends SqlViewTemplate>(structBuilder: SelectStructBuilder<VT>, cb: (column: Column) => Column) => {
	let instanceUsed = false
	return {
		instanceUsed: () => instanceUsed,
		template: iterateTemplate(structBuilder.template, (c) => c instanceof Column, (inner) => {
			throw 'todo'
		}) as VT,
		emitInnerUsed: () => {
			throw 'todo'
		},
		buildBody: (opts: {
			flag: BuildFlag,
			bracketIf: (sqlBody: SelectBodyStruct) => boolean
		}): SelectBodyStruct => {
			throw 'todo'
		}
	}
}

function createJoin<N extends boolean, VT1 extends SqlViewTemplate, VT extends SqlViewTemplate>(opts: {
	mode: 'left' | 'inner',
	withNull: N,
	lazy: boolean,
	lateral: boolean,
	baseBuilder: SelectStructBuilder<VT1>,
	extraBuilder: SelectStructBuilder<VT>,
	getCondationSegment: (opts: {
		base: VT1,
		extra: Relation<N, VT>,
	}) => Segment,
}) {

	const base = proxyBuilder(opts.baseBuilder, (c) => c)
	const extra = proxyBuilder(opts.extraBuilder, (c) => c.withNull(opts.withNull))
	const template = {
		base: base.template,
		extra: extra.template as Relation<N, VT>,
	}
	const condationBuilderCtx = opts.getCondationSegment(template).createBuilderCtx()

	return {
		template,
		emitInnerUsed: () => {
			if (extra.instanceUsed()) {
				condationBuilderCtx.emitInnerUsed()
				extra.emitInnerUsed()
			}
			base.emitInnerUsed()
		},
		buildBody: (flag: BuildFlag) => {
			if (!extra.instanceUsed()) {
				return base.buildBody({
					flag,
					bracketIf: () => false
				})
			}
			const baseBody = base.buildBody({
				flag,
				bracketIf: (sqlBody) => hasOneOf(sqlBody.state(), ['groupBy', 'having', 'order', 'skip', 'take']),
			})
			const extraBody = extra.buildBody({
				flag: {
					order: flag.order,
				},
				bracketIf: (sqlBody) => pickConfig(opts.mode, {
					left: () => hasOneOf(sqlBody.state(), ['innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
					inner: () => hasOneOf(sqlBody.state(), ['leftJoin', 'groupBy', 'having', 'order', 'skip', 'take']),
				})
			})

			return new SelectBodyStruct({
				from: baseBody.opts.from,
				join: [
					...baseBody.opts.join,
					{
						type: opts.mode,
						lateral: opts.lateral,
						expr: extraBody.opts.from.expr,
						alias: extraBody.opts.from.alias,
						condation: condationBuilderCtx.buildExpr(),
					},
					...extraBody.opts.join,
				],
				where: [
					...baseBody.opts.where ?? [],
					...extraBody.opts.where ?? [],
				],
				groupBy: [],
				having: [],
				order: baseBody.opts.order,
				take: baseBody.opts.take,
				skip: baseBody.opts.skip,
			})
		},
	}
}

export class SqlView<const VT1 extends SqlViewTemplate> extends InnerClass {
	[innerTypeSym] = 'sqlView' as const
	constructor(
		public readonly _createStructBuilder: () => SelectStructBuilder<VT1>,
	) {
		super()
	}

	pipe<R>(op: (self: this) => R): R {
		return op(this)
	}

	createSelectAll(flag: BuildFlag) {
		const structBuilder = this._createStructBuilder()
		iterateTemplate(structBuilder.template, (c) => c instanceof Column, (c) => {
			const columnOpts = Column.getOpts(c)
			columnOpts.builderCtx.emitInnerUsed()
		})
		structBuilder.emitInnerUsed()
		const { selectTarget, columnFormatMapper } = exec(() => {
			const exprTools = createExprTools()
			const allSelectTarget: Map<object, {
				expr: ActiveExpr[],
				alias: Holder,
			}> = new Map()
			const columnFormatMapper: Map<Column, (helper: BuildSqlHelper, raw: { [key: string]: unknown }) => Promise<unknown>> = new Map()
			iterateTemplate(structBuilder.template, (c) => c instanceof Column, (c) => {
				const columnOpts = Column.getOpts(c)
				const columnExpr = columnOpts.builderCtx.buildExpr()
				const columnExprKey = exprTools.fetchExprKey(columnExpr)
				if (!allSelectTarget.has(columnExprKey)) {
					const aliasHolder = new Holder((helper) => helper.fetchColumnAlias(columnExprKey))
					allSelectTarget.set(columnExprKey, {
						expr: columnExpr,
						alias: aliasHolder,
					})
				}
				if (!columnFormatMapper.has(c)) {
					columnFormatMapper.set(c, async (helper, raw) => {
						const aliasStr = helper.fetchColumnAlias(columnExprKey)
						if (columnOpts.withNull === true && raw[aliasStr] === null) { return null }
						return columnOpts.format(raw[aliasStr])
					})
				}
			})
			return {
				selectTarget: new Map([...allSelectTarget.values()].map(({ expr, alias }) => [alias, expr])),
				columnFormatMapper,
			}
		})

		const bodyExpr = structBuilder.buildBody(flag).getBodyExpr()
		return {
			expr: [
				'select ',
				getSelectAliasExpr(selectTarget),
				' from ',
				bodyExpr,
			].flat(1) satisfies ActiveExpr[],
			rawFormatter: async (helper: BuildSqlHelper, raw: { [key: string]: unknown }) => {
				const loadingArr: Promise<unknown>[] = new Array()
				const resultMapper = new Map<Column, unknown>()
				iterateTemplate(structBuilder.template, (c) => c instanceof Column, (c) => {
					const formatMapper = columnFormatMapper.get(c)
					if (!formatMapper) { throw new Error('formatter not found') }
					loadingArr.push(formatMapper(helper, raw).then((value) => {
						resultMapper.set(c, value)
					}))
				})
				await Promise.all(loadingArr)
				return iterateTemplate(structBuilder.template, (c) => c instanceof Column, (c) => resultMapper.get(c)) as SelectResult<VT1>
			}
		}
	}

	andWhere(getCondation: (
		template: VT1,
	) => null | false | undefined | Segment): SqlView<VT1> {
		return new SqlView(() => {
			const instance = proxyBuilder<VT1>(this._createStructBuilder(), (c) => c)
			const condationBuilderCtx = exec(() => {
				const condationSegment = getCondation(instance.template)
				return !condationSegment ? null : condationSegment.createBuilderCtx()
			})
			if (!condationBuilderCtx) {
				return {
					template: instance.template,
					emitInnerUsed: () => {
						instance.emitInnerUsed()
					},
					buildBody: (flag) => {
						return instance.buildBody({
							flag,
							bracketIf: () => false,
						})
					}
				}
			}
			return {
				template: instance.template,
				emitInnerUsed: () => {
					if (condationBuilderCtx) {
						condationBuilderCtx.emitInnerUsed()
					}
					instance.emitInnerUsed()
				},
				buildBody: (flag) => {
					const condationExpr = condationBuilderCtx.buildExpr()
					if (condationExpr.length === 0) {
						return instance.buildBody({
							flag,
							bracketIf: () => false,
						})
					}
					const sqlBody = instance.buildBody({
						flag,
						bracketIf: (sqlBody) => hasOneOf(sqlBody.state(), ['take', 'skip'])
					})
					const target = sqlBody.opts.groupBy.length === 0 ? sqlBody.opts.where : sqlBody.opts.having
					target.push({
						expr: condationExpr,
					})
					return sqlBody
				},
			}
		})
	}

	groupBy<const VT extends SqlViewTemplate>(
		getKeyTemplate: (vt: VT1) => SqlViewTemplate,
		getTemplate: (vt: VT1) => VT,
	): SqlView<VT> {
		return new SqlView(() => {
			const instance = proxyBuilder<VT1>(this._createStructBuilder(), (c) => c)
			const keys = getKeyTemplate(instance.template)
			const content = getTemplate(instance.template)
			return {
				template: content,
				decalerUsedExpr: instance.decalerUsedExpr,
				getSqlBody: (flag) => {
					const sqlBody = instance.getSqlBody({
						flag,
						bracketIf: (sqlBody) => hasOneOf(sqlBody.state(), ['order', 'groupBy', 'having', 'skip', 'take'])
					})
					iterateTemplate(keys, (c) => c instanceof Column, (c) => {
						instance.decalerUsedExpr(c[sym].expr)
						sqlBody.opts.groupBy.push(c[sym].expr)
					})
					return sqlBody
				}
			}
		})
	}


	join<
		M extends 'left join' | 'inner join' | 'left join withNull' | 'left join lazy' | 'left join lazy withNull',
		const VT extends SqlViewTemplate
	>(mode: M, view: SqlView<VT>) {
		type N = M extends `${string}withNull${string}` ? true : false
		const isWithNull = mode.includes('withNull') as N
		const joinMode = mode.includes('left') ? 'left' : 'inner'
		return {
			on: (
				getCondationSegment: (opts: {
					base: VT1,
					extra: Relation<N, VT>
				}) => Segment,
			) => {
				return new SqlView<{ base: VT1, extra: Relation<N, VT> }>(() => {
					const baseBuilder = this._createStructBuilder()
					const extraBuilder = view._createStructBuilder()
					return createJoin<N, VT1, VT>({
						mode: joinMode,
						withNull: isWithNull,
						lazy: mode.includes('lazy'),
						lateral: false,
						baseBuilder,
						extraBuilder,
						getCondationSegment: getCondationSegment,
					})
				})
			}
		}

	}

	lateralJoin<
		M extends 'left join' | 'inner join' | 'left join withNull' | 'left join lazy' | 'left join lazy withNull',
		const VT extends SqlViewTemplate,
	>(mode: M, getView: (template: VT1) => SqlView<VT>) {
		type N = M extends `${string}withNull${string}` ? true : false
		const isWithNull = mode.includes('withNull') as N
		const joinMode = mode.includes('left') ? 'left' : 'inner'
		return {
			on: (
				getCondationSegment: (opts: {
					base: VT1,
					extra: Relation<N, VT>
				}) => Segment,
			) => {
				return new SqlView<{ base: VT1, extra: Relation<N, VT> }>(() => {
					const baseBuilder = this._createStructBuilder()
					const extraBuilder = getView(baseBuilder.template)._createStructBuilder()
					return createJoin<N, VT1, VT>({
						mode: joinMode,
						withNull: isWithNull,
						lazy: mode.includes('lazy'),
						lateral: true,
						baseBuilder,
						extraBuilder,
						getCondationSegment: getCondationSegment,
					})
				})
			}
		}
	}

	mapTo<const VT extends SqlViewTemplate>(getTemplate: (e: VT1, opts: {
		decalreUsed: (expr: string | Column<boolean, unknown>) => void,
		param: SetParam,
	}) => VT): SqlView<VT> {
		return new SqlView((tools) => {
			const base = this._createStructBuilder(tools)
			return {
				template: getTemplate(base.template, {
					decalreUsed: (expr) => {
						base.decalerUsedExpr(expr.toString())
					},
					param: tools.setParam,
				}),
				decalerUsedExpr: base.decalerUsedExpr,
				getSqlBody: base.getSqlStruct,
			}
		})
	}

	bracketIf(condation: (sqlBody: SelectBodyStruct) => boolean) {
		return new SqlView<VT1>((tools) => {
			const instance = proxyBuilder<VT1>(tools, this._createStructBuilder(tools), (c) => c)
			return {
				template: instance.template,
				decalerUsedExpr: instance.decalerUsedExpr,
				getSqlBody: (flag) => instance.getSqlBody({
					flag,
					bracketIf: (sqlBody) => condation(sqlBody),
				}),
			}
		})
	}

	order(
		order: 'asc' | 'desc',
		getExpr: (template: VT1, param: SetParam) => false | null | undefined | string | Column<boolean, unknown>,
	): SqlView<VT1> {
		return new SqlView((tools) => {
			const instance = proxyBuilder<VT1>(tools, this._createStructBuilder(tools), (c) => c)
			return {
				template: instance.template,
				decalerUsedExpr: instance.decalerUsedExpr,
				getSqlBody: (flag) => {
					if (!flag.order) {
						return instance.getSqlBody({
							flag,
							bracketIf: () => false,
						})
					}
					let expr = getExpr(instance.template, tools.setParam)?.toString()
					if (expr) { expr = expr.trim() }
					if (!expr) {
						return instance.getSqlBody({
							flag,
							bracketIf: () => false,
						})
					}
					instance.decalerUsedExpr(expr)
					const sqlBody = instance.getSqlBody({
						flag,
						bracketIf: (sqlBody) => hasOneOf(sqlBody.state(), ['skip', 'take']),
					})
					sqlBody.opts.order.unshift({
						order,
						expr,
					})
					return sqlBody
				},
			}
		})
	}

	take(count: number | null | undefined | false): SqlView<VT1> {
		if (typeof count !== 'number') { return this }
		return new SqlView((tools) => {
			const instance = this._createStructBuilder(tools)
			return {
				template: instance.template,
				decalerUsedExpr: instance.decalerUsedExpr,
				getSqlBody: (flag) => {
					const sqlBody = instance.getSqlStruct({
						...flag,
						order: true,
					})
					sqlBody.opts.take = sqlBody.opts.take === null ? count : Math.min(sqlBody.opts.take, count)
					return sqlBody
				},
			}
		})
	}

	skip(count: number | null | undefined | false): SqlView<VT1> {
		if (typeof count !== 'number' || count <= 0) { return this }
		return new SqlView((tools) => {
			const instance = this
				.bracketIf((sqlBody) => hasOneOf(sqlBody.state(), ['take']))
				._createStructBuilder(tools)
			return {
				template: instance.template,
				decalerUsedExpr: instance.decalerUsedExpr,
				getSqlBody: (flag) => {
					const sqlBody = instance.getSqlStruct({
						...flag,
						order: true,
					})
					sqlBody.opts.skip = sqlBody.opts.skip + count
					sqlBody.opts.take = sqlBody.opts.take === null ? null : Math.max(0, sqlBody.opts.take - count)
					return sqlBody
				},
			}
		})
	}
};