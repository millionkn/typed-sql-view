import { ColumnRef, Relation, SelectBodyStruct, SqlViewTemplate, ActiveExpr, Segment, Holder, SqlState, sym, parseFullSelectExpr, AssertWithNull } from "./define.js"
import { exec, hasOneOf, iterateTemplate, pickConfig } from "./tools.js"

export type BuildFlag = {
	order: boolean,
}


export type SelectStructBuilder<VT extends SqlViewTemplate> = {
	template: VT,
	emitInnerUsed: () => void,
	finalize: (flag: BuildFlag) => SelectBodyStruct,
}

export type SelectResult<VT extends SqlViewTemplate> = VT extends readonly [] ? []
	: VT extends readonly [infer X extends SqlViewTemplate, ...infer Arr extends readonly SqlViewTemplate[]]
	? [SelectResult<X>, ...SelectResult<Arr>]
	: VT extends readonly (infer X extends SqlViewTemplate)[]
	? SelectResult<X>[]
	: VT extends ColumnRef<infer N, infer R>
	? (true extends N ? null : never) | R
	: VT extends { [key: string]: SqlViewTemplate }
	? { -readonly [key in keyof VT]: SelectResult<VT[key]> }
	: never

export const proxyBuilder = <VT extends SqlViewTemplate>(structBuilder: SelectStructBuilder<VT>, forceSetNull: boolean) => {
	let instanceUsed = false
	const outerUsedMapper: Map<symbol, {
		backupAlias: Holder,
		buildInnerExpr: () => ActiveExpr[],
		buildOuterExpr: () => ActiveExpr[],
	}> = new Map()
	return {
		instanceUsed: () => instanceUsed,
		template: iterateTemplate(structBuilder.template, (c) => c instanceof ColumnRef, (inner) => {
			const opts = ColumnRef.getOpts(inner)
			const key = Symbol('columnAlias')
			const backupAlias = new Holder((helper) => helper.fetchColumnAlias(key))
			return new ColumnRef({
				builderCtx: {
					emitInnerUsed: () => {
						instanceUsed = true
						opts.builderCtx.emitInnerUsed()
						outerUsedMapper.set(key, {
							backupAlias,
							buildInnerExpr: () => opts.builderCtx.buildExpr(),
							buildOuterExpr: () => opts.builderCtx.buildExpr(),
						})
					},
					buildExpr: () => {
						return outerUsedMapper.get(key)!.buildOuterExpr()
					},
				},
				format: opts.format,
				withNull: forceSetNull ? true : opts.withNull,
			})
		}) as VT,
		emitInnerUsed: () => {
			instanceUsed = true
			structBuilder.emitInnerUsed()
		},
		finalize: (opts: {
			flag: BuildFlag,
			bracketIf: (bodyOpts: { state: Set<SqlState> }) => boolean,
		}) => {
			const sqlBody = structBuilder.finalize(opts.flag)
			if (!opts.bracketIf({ state: sqlBody.state() })) {
				return sqlBody
			}
			const tableAlias = exec(() => {
				const key = Symbol('tableAlias')
				return new Holder((helper) => helper.fetchTableAlias(key))
			})
			const result = sqlBody.bracket(tableAlias, new Map([...outerUsedMapper.values()].map((e) => {
				return [e.backupAlias, {
					buildExpr: () => e.buildInnerExpr()
				}]
			})))
			outerUsedMapper.forEach((e) => {
				const holder = new Holder((helper) => helper.adapter.columnRef(
					tableAlias.effectOn(helper),
					e.backupAlias.effectOn(helper),
				))
				e.buildOuterExpr = () => [holder]
			})
			return result
		},
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
}): SelectStructBuilder<{ base: VT1, extra: Relation<N, VT> }> {
	const base = proxyBuilder(opts.baseBuilder, false)
	const extra = proxyBuilder(opts.extraBuilder, opts.withNull)
	const template = {
		base: base.template,
		extra: extra.template as Relation<N, VT>,
	}
	const condationBuilderCtx = opts.getCondationSegment(template)[sym].createBuilderCtx()

	return {
		template,
		emitInnerUsed: () => {
			if (extra.instanceUsed()) {
				condationBuilderCtx.emitInnerUsed()
				extra.emitInnerUsed()
			}
			base.emitInnerUsed()
		},
		finalize: (flag: BuildFlag) => {
			if (!extra.instanceUsed()) {
				return base.finalize({
					flag,
					bracketIf: () => false,
				})
			}
			const baseBody = base.finalize({
				flag,
				bracketIf: (bodyOpts) => hasOneOf(bodyOpts.state, ['groupBy', 'having', 'order', 'skip', 'take']),
			})
			const extraBody = extra.finalize({
				flag: {
					order: flag.order,
				},
				bracketIf: (bodyOpts) => pickConfig(opts.mode, {
					left: () => hasOneOf(bodyOpts.state, ['innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
					inner: () => hasOneOf(bodyOpts.state, ['leftJoin', 'groupBy', 'having', 'order', 'skip', 'take']),
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

function createMapTools() {
	return {
		assertWithNull: <N extends boolean, VT2 extends SqlViewTemplate>(withNull: N, template: VT2) => iterateTemplate(template, (c) => c instanceof ColumnRef, (c) => {
			return c.withNull(withNull)
		}) as AssertWithNull<N, VT2>,
		createColumn: (segment: Segment) => new ColumnRef({
			withNull: true,
			format: async (raw) => raw,
			builderCtx: segment[sym].createBuilderCtx(),
		})
	}
}


export class SqlView<const VT1 extends SqlViewTemplate> extends Segment {
	[sym] = {
		type: 'sqlView' as const,
		createStructBuilder: () => this.createStructBuilder(),
		createBuilderCtx: () => {
			const selectMapper: Map<Holder, {
				buildExpr: () => ActiveExpr[],
			}> = new Map()
			const builder = this.createStructBuilder()
			return {
				emitInnerUsed: () => {
					iterateTemplate(builder.template, (c) => c instanceof ColumnRef, (c) => {
						const builderCtx = ColumnRef.getOpts(c).builderCtx
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
					return parseFullSelectExpr({
						selectTarget: selectMapper,
						struct: struct,
					})
				},
			}
		},
	}
	constructor(
		private readonly createStructBuilder: () => SelectStructBuilder<VT1>,
	) {
		super()
	}

	pipe<R>(op: (self: this) => R): R {
		return op(this)
	}


	andWhere(getCondation: (
		template: VT1,
	) => null | false | undefined | '' | Segment): SqlView<VT1> {
		return new SqlView(() => {
			const instance = proxyBuilder<VT1>(this.createStructBuilder(), false)
			const condationBuilderCtx = exec(() => {
				const condationSegment = getCondation(instance.template)
				return !condationSegment ? null : condationSegment[sym].createBuilderCtx()
			})
			if (!condationBuilderCtx) {
				return {
					template: instance.template,
					emitInnerUsed: () => {
						instance.emitInnerUsed()
					},
					finalize: (flag) => {
						return instance.finalize({
							flag,
							bracketIf: () => false,
						})
					}
				}
			}
			return {
				template: instance.template,
				emitInnerUsed: () => {
					condationBuilderCtx.emitInnerUsed()
					instance.emitInnerUsed()
				},
				finalize: (flag) => {
					const sqlBody = instance.finalize({
						flag,
						bracketIf: (bodyOpts) => hasOneOf(bodyOpts.state, ['take', 'skip'])
					})
					const condationExpr = condationBuilderCtx.buildExpr()
					const target = sqlBody.opts.groupBy.length === 0 ? sqlBody.opts.where : sqlBody.opts.having
					target.push({
						expr: condationExpr,
					})
					return sqlBody
				},
			}
		})
	}

	groupBy<const KT extends SqlViewTemplate, const VT extends SqlViewTemplate>(
		getGroupBy: (vt: VT1, tools: ReturnType<typeof createMapTools>) => KT,
		getSelect: (keys: KT, tools: ReturnType<typeof createMapTools>) => VT,
	) {
		return new SqlView(() => {
			const instance = proxyBuilder<VT1>(this.createStructBuilder(), false)
			const keys = getGroupBy(instance.template, createMapTools())
			let noKeys = true
			const selectTarget = getSelect(keys, createMapTools())
			return {
				template: selectTarget,
				emitInnerUsed: () => {
					iterateTemplate(keys, (c) => c instanceof ColumnRef, (c) => {
						noKeys = false
						ColumnRef.getOpts(c).builderCtx.emitInnerUsed()
					})
					iterateTemplate(selectTarget, (c) => c instanceof ColumnRef, (c) => {
						ColumnRef.getOpts(c).builderCtx.emitInnerUsed()
					})
					instance.emitInnerUsed()
				},
				finalize: () => {
					if (noKeys) {
						return instance.finalize({
							flag: {
								order: false
							},
							bracketIf: (bodyOpts) => bodyOpts.state.size !== 0,
						})
					}
					const sqlBody = instance.finalize({
						flag: {
							order: false,
						},
						bracketIf: (bodyOpts) => hasOneOf(bodyOpts.state, ['order', 'groupBy', 'having', 'skip', 'take'])
					})
					iterateTemplate(keys, (c) => c instanceof ColumnRef, (c) => {
						sqlBody.opts.groupBy.push({
							expr: ColumnRef.getOpts(c).builderCtx.buildExpr(),
						})
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
					const baseBuilder = this.createStructBuilder()
					const extraBuilder = view.createStructBuilder()
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
					const baseBuilder = this.createStructBuilder()
					const extraBuilder = getView(baseBuilder.template).createStructBuilder()
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

	mapTo<const VT extends SqlViewTemplate>(getTemplate: (
		e: VT1,
		tools: ReturnType<typeof createMapTools>,
	) => VT): SqlView<VT> {
		return new SqlView(() => {
			const base = this.createStructBuilder()
			return {
				template: getTemplate(base.template, createMapTools()),
				emitInnerUsed: base.emitInnerUsed,
				finalize: base.finalize,
			}
		})
	}

	bracketIf(condation: (opts: {
		state: Set<SqlState>,
	}) => boolean) {
		return new SqlView<VT1>(() => {
			const instance = proxyBuilder<VT1>(this.createStructBuilder(), false)
			return {
				template: instance.template,
				emitInnerUsed: instance.emitInnerUsed,
				finalize: (flag) => instance.finalize({
					flag,
					bracketIf: (bodyOpts) => condation({
						state: bodyOpts.state,
					}),
				}),
			}
		})
	}

	order(
		order: `ASC` | `DESC` | 'asc' | 'desc',
		nulls: `FIRST` | `LAST` | 'first' | 'last',
		getExpr: (template: VT1) => false | null | undefined | Segment,
	): SqlView<VT1> {
		return new SqlView(() => {
			const instance = proxyBuilder<VT1>(this.createStructBuilder(), false)
			const builderCtx = exec(() => {
				const expr = getExpr(instance.template)
				if (!expr) {
					return null
				} if (expr instanceof Segment) {
					return expr[sym].createBuilderCtx()
				} else {
					throw expr satisfies never
				}
			})
			return {
				template: instance.template,
				emitInnerUsed: () => {
					builderCtx?.emitInnerUsed()
					instance.emitInnerUsed()
				},
				finalize: (flag) => {
					if (!flag.order || !builderCtx) {
						return instance.finalize({
							flag,
							bracketIf: () => false,
						})
					}
					const sqlBody = instance.finalize({
						flag,
						bracketIf: (bodyOpts) => hasOneOf(bodyOpts.state, ['skip', 'take']),
					})
					sqlBody.opts.order.unshift({
						order: order.toUpperCase() as 'ASC' | 'DESC',
						nulls: nulls.toUpperCase() as 'FIRST' | 'LAST',
						expr: builderCtx.buildExpr(),
					})
					return sqlBody
				},
			}
		})
	}

	take(count: number | null | undefined | false): SqlView<VT1> {
		if (typeof count !== 'number') { return this }
		return new SqlView(() => {
			const instance = this.createStructBuilder()
			return {
				template: instance.template,
				emitInnerUsed: instance.emitInnerUsed,
				finalize: (flag) => {
					const sqlBody = instance.finalize({
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
		return new SqlView(() => {
			const instance = this
				.bracketIf((opts) => hasOneOf(opts.state, ['take']))
				.createStructBuilder()
			return {
				template: instance.template,
				emitInnerUsed: instance.emitInnerUsed,
				finalize: (flag) => {
					const sqlBody = instance.finalize({
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