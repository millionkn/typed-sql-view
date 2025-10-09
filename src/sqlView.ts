import { Column, Relation, SelectBodyStruct, SqlViewTemplate, iterateTemplate, BuildSqlHelper, ActiveExpr, InnerClass, innerTypeSym, Segment, Holder, createExprTools } from "./define.js"
import { exec } from "./tools.js"

export type BuildFlag = {
	order: boolean,
}


export type SelectStructBuilder<VT extends SqlViewTemplate> = {
	template: VT,
	emitInnerUsed: () => void,
	buildStruct: (flag: BuildFlag) => SelectBodyStruct,
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
		template: iterateTemplate(structBuilder.template, (inner) => {
			const columnOpts = Column.getOpts(inner)
			const outer = cb(new Column({
				withNull: columnOpts.withNull,
				format: columnOpts.format,
				builderCtx: {
					buildExpr: () => {

					},
					emitInnerUsed: () => {
						instanceUsed = true
						columnOpts.builderCtx.emitInnerUsed()
					}
				}
			}))
			outer[sym].expr = holder.expr
			info.set(holder.expr, {
				used: false,
				inner,
				outer,
				replaceWith: holder.replaceWith,
			})
			return outer
		}) as VT,
		getSqlStruct: (opts: {
			flag: BuildFlag,
			bracketIf: (sqlBody: SelectBodyStruct) => boolean
		}) => {
			let sqlBody = structBuilder.getSqlStruct(opts.flag, opts.usedColumn)
			if (!opts.bracketIf(sqlBody)) {
				info.forEach((v) => {
					if (!v.used) { return }
					v.replaceWith(v.inner[sym].expr)
				})
				return sqlBody
			}
			const tableAlias = tools.genAlias()
			const selectTarget = [] as {
				expr: string,
				alias: string,
			}[]
			info.forEach((v) => {
				if (!v.used) { return }
				const alias = `value_${selectTarget.length + 1}`
				v.replaceWith(`${tableAlias}.${alias}`)
				selectTarget.push({
					expr: v.inner[sym].expr,
					alias,
				})
			})
			return sqlBody.bracket({
				tableAlias,
				selectTarget,
			})

		}
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

	createSnapshot() {
		const structBuilder = this._createStructBuilder()
		iterateTemplate(structBuilder.template, (c) => {
			const columnOpts = Column.getOpts(c)
			columnOpts.builderCtx.emitInnerUsed()
		})
		const snapshot = structBuilder.snapshot()
		return {
		}
	}

	createSelectAll(flag: BuildFlag) {
		const structBuilder = this._createStructBuilder()
		iterateTemplate(structBuilder.template, (c) => {
			const columnOpts = Column.getOpts(c)
			columnOpts.builderCtx.emitInnerUsed()
		})
		const snapshot = structBuilder.snapshot()
		const { selectTarget, columnFormatMapper } = exec(() => {
			const exprTools = createExprTools()
			const allSelectTarget: Map<object, {
				expr: ActiveExpr[],
				alias: Holder,
			}> = new Map()
			const columnFormatMapper: Map<Column, (helper: BuildSqlHelper, raw: { [key: string]: unknown }) => Promise<unknown>> = new Map()
			iterateTemplate(structBuilder.template, (c) => {
				const columnOpts = Column.getOpts(c)
				const columnExpr = columnOpts.builderCtx.toExpr()
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

		const sqlExpr = SelectBodyStruct.getExpr(snapshot.getStruct(flag), selectTarget)


		return {
			expr: sqlExpr,
			rawFormatter: async (helper: BuildSqlHelper, raw: { [key: string]: unknown }) => {
				const loadingArr: Promise<unknown>[] = new Array()
				const resultMapper = new Map<Column, unknown>()
				iterateTemplate(structBuilder.template, (c) => {
					const formatMapper = columnFormatMapper.get(c)
					if (!formatMapper) { throw new Error('formatter not found') }
					loadingArr.push(formatMapper(helper, raw).then((value) => {
						resultMapper.set(c, value)
					}))
				})
				await Promise.all(loadingArr)
				return iterateTemplate(structBuilder.template, (c) => resultMapper.get(c)) as SelectResult<VT1>
			}
		}
	}

	andWhere(getCondation: (
		template: VT1,
	) => null | false | undefined | '' | UserSegment): SqlView<VT1> {
		return new SqlView(() => {
			const instance = proxyBuilder<VT1>(tools, this._createStructBuilder(tools), (c) => c)

			return {
				template: instance.template,
				getSqlStruct: (flag, usedColumnArr) => {
					const condationExpr = getCondation(instance.template)
					if (!condationExpr) {
						return instance.getSqlBody({
							flag,
							bracketIf: () => false,
						})
					}
					instance.decalerUsedExpr(condationExpr[sym2]())
					const sqlBody = instance.getSqlBody({
						flag,
						bracketIf: (sqlBody) => hasOneOf(sqlBody.state(), ['take', 'skip'])
					})

					const target = sqlBody.opts.groupBy.length === 0 ? sqlBody.opts.where : sqlBody.opts.having
					target.push(condationExpr)
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
					iterateTemplate(keys, (c) => {
						instance.decalerUsedExpr(c[sym].expr)
						sqlBody.opts.groupBy.push(c[sym].expr)
					})
					return sqlBody
				}
			}
		})
	}

	joinLazy<const VT extends SqlViewTemplate>(getTemplate: (e: VT1, opts: {
		leftJoin: <N extends boolean, VT extends SqlViewTemplate>(withNull: N, view: SqlView<VT>, getCondationExpr: (t: Relation<N, VT>, param: SetParam) => string) => Relation<N, VT>,
	}) => VT): SqlView<VT> {
		return new SqlView((tools) => {
			const base = proxyBuilder<VT1>(tools, this._createStructBuilder(tools), (c) => c)
			const extraArr: Array<{
				instance: ReturnType<typeof proxyBuilder<SqlViewTemplate>>,
				getCondationExpr: () => string,
			}> = []
			return {
				template: getTemplate(base.template, {
					leftJoin: (withNull, view, getCondationExpr) => {
						type R = Parameters<typeof getCondationExpr>[0]
						const proxy = proxyBuilder<R>(tools, view._createStructBuilder(tools) as SelectStructBuilder<R>, (c) => {
							c[sym].withNull ||= withNull
							return c
						})
						extraArr.push({
							instance: proxy,
							getCondationExpr: () => getCondationExpr(proxy.template, tools.setParam)
						})
						return proxy.template
					},
				}),
				decalerUsedExpr: (expr) => {
					base.decalerUsedExpr(expr)
					extraArr.forEach((e) => e.instance.decalerUsedExpr(expr))
				},
				getSqlBody: (flag) => {
					const usedExtraArr = extraArr.filter((e) => e.instance.instanceUsed())
					if (usedExtraArr.length === 0) {
						base.info.forEach((e) => {
							e.replaceWith(e.inner[sym].expr)
						})
						return base.getSqlBody({ flag, bracketIf: () => false })
					}
					const arr = usedExtraArr.map(({ getCondationExpr, instance }, index, arr) => {
						const condationExpr = getCondationExpr()
						base.decalerUsedExpr(condationExpr)
						arr.slice(0, index + 1).forEach((e) => e.instance.decalerUsedExpr(condationExpr))
						let body = null as null | SelectBodyStruct
						return {
							condationExpr,
							getBody: () => body ||= instance.getSqlBody({
								flag: {
									order: false,
								},
								bracketIf: (sqlBody) => hasOneOf(sqlBody.state(), ['leftJoin', 'innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take'])
							})
						}
					})
					const baseBody = base.getSqlBody({
						flag,
						bracketIf: (sqlBody) => hasOneOf(sqlBody.state(), ['groupBy', 'having', 'order', 'skip', 'take']),
					})
					return new SelectBodyStruct({
						from: baseBody.opts.from,
						join: [
							...baseBody.opts.join,
							...arr.flatMap(({ getBody, condationExpr }) => {
								return [
									...getBody().opts.from.map((info) => {
										return {
											type: 'left' as const,
											alias: info.alias,
											expr: info.expr,
											condation: condationExpr,
										}
									}),
									...getBody().opts.join,
								]
							}),
						],
						where: [
							...baseBody.opts.where ?? [],
							...arr.flatMap(({ getBody }) => {
								return getBody().opts.where
							})
						],
						groupBy: [],
						having: [],
						order: baseBody.opts.order,
						take: baseBody.opts.take,
						skip: baseBody.opts.skip,
					})
				},
			}
		})
	}

	join<
		M extends 'left join' | 'inner join' | 'left join withNull' | 'left join lazy' | 'left join lazy withNull',
		const VT extends SqlViewTemplate
	>(mode: M, view: SqlView<VT>) {
		return {
			on: (
				getCondationExpr: (opts: {
					base: VT1,
					extra: Relation<M extends `${string}withNull${string}` ? true : false, VT>
				}) => Segment,
			): SqlView<{ base: VT1, extra: VT }> => {
				const isWithNull = mode.includes('withNull')
				return new SqlView(() => {
					const base = this._createStructBuilder()
					const extra = proxyBuilder(view._createStructBuilder(), (c) => c.withNull(isWithNull))
					const template = {
						base: base.template,
						extra: extra.template satisfies VT as Relation<M extends `${string}withNull${string}` ? true : false, VT>,
					}
					const condationExpr = getCondationExpr(template)
					return {
						template,
						getSqlStruct: (flag, usedSegment) => {

							const usedExtraArr = extraArr
							if (usedExtraArr.length === 0) {
								base.info.forEach((e) => {
									e.replaceWith(e.inner[sym].expr)
								})
								return base.getSqlBody({ flag, bracketIf: () => false })
							}
							const arr = usedExtraArr.map(({ getCondationExpr, instance, mode }, index, arr) => {
								const condationExpr = getCondationExpr()
								base.decalerUsedExpr(condationExpr)
								arr.slice(0, index + 1).forEach((e) => e.instance.decalerUsedExpr(condationExpr))
								let body = null as null | SelectBodyStruct
								return {
									condationExpr,
									mode,
									getBody: () => body ||= instance.getSqlBody({
										flag: {
											order: flag.order,
										},
										bracketIf: (sqlBody) => pickConfig(mode, {
											left: () => hasOneOf(sqlBody.state(), ['leftJoin', 'innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
											inner: () => hasOneOf(sqlBody.state(), ['leftJoin', 'innerJoin', 'groupBy', 'having', 'order', 'skip', 'take']),
										})
									})
								}
							})
							const baseBody = base.getSqlBody({
								flag,
								bracketIf: (sqlBody) => hasOneOf(sqlBody.state(), ['groupBy', 'having', 'order', 'skip', 'take']),
							})
							return new SelectBodyStruct({
								from: baseBody.opts.from,
								join: [
									...baseBody.opts.join,
									...arr.flatMap(({ getBody, mode, condationExpr }) => {
										return [
											...getBody().opts.from.map((info) => {
												return {
													type: mode,
													alias: info.alias,
													expr: info.expr,
													condation: condationExpr,
												}
											}),
											...getBody().opts.join,
										]
									}),
								],
								where: [
									...baseBody.opts.where ?? [],
									...arr.flatMap(({ getBody }) => {
										return getBody().opts.where
									})
								],
								groupBy: [],
								having: [],
								order: baseBody.opts.order,
								take: baseBody.opts.take,
								skip: baseBody.opts.skip,
							})
						},
					}
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
		return new SqlView((tools) => {
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