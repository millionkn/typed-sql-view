import { BuildTools, Column, Relation, SelectSqlStruct, SqlViewTemplate, iterateTemplate, hasOneOf, pickConfig, sym, SetParam, exec, createResolver, Adapter, SqlSegment, RawSqlSegment, sym2, SqlSegmentLike } from "./define.js"

export type BuildFlag = {
	order: boolean,
}


export type RuntimeInstance<VT extends SqlViewTemplate> = {
	template: VT,
	decalerUsedExpr: (expr: string) => void,
	getSqlStruct: (flag: BuildFlag) => SelectSqlStruct,
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

export const proxyInstance = <VT extends SqlViewTemplate>(tools: BuildTools, instance: RuntimeInstance<VT>, cb: (column: Column<boolean, unknown>) => Column<boolean, unknown>) => {
	const info: Map<string, {
		used: boolean,
		inner: Column<boolean, unknown>,
		outer: Column<boolean, unknown>,
		replaceWith: (expr: string) => void,
	}> = new Map()
	let instanceUsed = false
	return {
		instanceUsed: () => instanceUsed,
		info,
		template: iterateTemplate(instance.template, (inner) => {
			const outer = cb(inner.withNull(inner[sym].withNull))
			const holder = tools.createHolder()
			outer[sym].expr = holder.expr
			info.set(holder.expr, {
				used: false,
				inner,
				outer,
				replaceWith: holder.replaceWith,
			})
			return outer
		}) as VT,
		decalerUsedExpr: (expr: RawSqlSegment) => expr.split(`''""''""`).forEach((key, i) => {
			if (i % 2 === 0) { return }
			const e = info.get(`''""''""${key}''""''""`)
			if (!e) { return }
			if (e.used) { return }
			e.used = true
			instanceUsed = true
			instance.decalerUsedExpr(e.inner[sym].expr)
		}),
		getSqlBody: (opts: {
			flag: BuildFlag,
			bracketIf: (sqlBody: SelectSqlStruct) => boolean
		}) => {
			let sqlBody = instance.getSqlStruct(opts.flag)
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

export class SqlView<const VT1 extends SqlViewTemplate> extends SqlSegmentLike {
	constructor(
		private _getInstance: () => RuntimeInstance<VT1>,
	) {
		super()

	}

	pipe<R>(op: (self: this) => R): R {
		return op(this)
	}

	buildSelectAll(
		flag: BuildFlag,
		adapter: Adapter,
	) {

		const paramArr = [] as unknown[]
		const viewInstance = this._getInstance()

		const selectTarget: Map<string, {
			alias: string,
			format: (raw: { [key: string]: unknown }) => Promise<unknown>,
		}> = new Map()

		iterateTemplate(viewInstance.template, (c) => {
			viewInstance.decalerUsedExpr(c)
		})
		const rawSql = viewInstance
			.getSqlStruct(flag)
			.buildSqlStr(adapter, [...selectTarget].map(([expr, { alias }]) => ({ expr, alias })))
		return {
			sql: resolver.resolve(rawSql),
			paramArr,
			rawFormatter: async (selectResult: { [key: string]: unknown }) => {
				const loadingArr: Promise<unknown>[] = new Array()
				const resultMapper = new Map<string, unknown>()
				iterateTemplate(viewInstance.template, (c) => {
					const expr = c[sym].expr
					if (!resultMapper.has(expr)) {
						loadingArr.push(selectTarget.get(expr)!.format(selectResult).then((value) => {
							resultMapper.set(expr, value)
						}))
					}
				})
				await Promise.all(loadingArr)
				return iterateTemplate(viewInstance.template, (c) => resultMapper.get(c[sym].expr)) as SelectResult<VT1>
			}
		}
	}

	andWhere(getCondation: (
		template: VT1,
	) => null | false | undefined | '' | SqlSegment): SqlView<VT1> {
		return new SqlView(() => {
			const instance = proxyInstance<VT1>(tools, this._getInstance(tools), (c) => c)

			return {
				template: instance.template,
				decalerUsedExpr: instance.decalerUsedExpr,
				getSqlBody: (flag) => {
					let condationExpr = getCondation(instance.template)
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
		getKeyTemplate: (vt: VT1, param: SetParam) => SqlViewTemplate,
		getTemplate: (vt: VT1, param: SetParam) => VT,
	): SqlView<VT> {
		return new SqlView((tools) => {
			const instance = proxyInstance<VT1>(tools, this._getInstance(tools), (c) => c)
			const keys = getKeyTemplate(instance.template, tools.setParam)
			const content = getTemplate(instance.template, tools.setParam)
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
			const base = proxyInstance<VT1>(tools, this._getInstance(tools), (c) => c)
			const extraArr: Array<{
				instance: ReturnType<typeof proxyInstance<SqlViewTemplate>>,
				getCondationExpr: () => string,
			}> = []
			return {
				template: getTemplate(base.template, {
					leftJoin: (withNull, view, getCondationExpr) => {
						type R = Parameters<typeof getCondationExpr>[0]
						const proxy = proxyInstance<R>(tools, view._getInstance(tools) as RuntimeInstance<R>, (c) => {
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
						let body = null as null | SelectSqlStruct
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
					return new SelectSqlStruct({
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

	join<const VT extends SqlViewTemplate>(getTemplate: (e: VT1, opts: {
		leftJoin: <N extends boolean, VT extends SqlViewTemplate>(withNull: N, view: SqlView<VT>, getCondationExpr: (t: Relation<N, VT>, param: SetParam) => string) => Relation<N, VT>,
		innerJoin: <VT extends SqlViewTemplate>(view: SqlView<VT>, getCondationExpr: (t: VT, param: SetParam) => string) => VT,
	}) => VT): SqlView<VT> {
		return new SqlView((tools) => {
			const base = proxyInstance<VT1>(tools, this._getInstance(tools), (c) => c)
			const extraArr: Array<{
				instance: ReturnType<typeof proxyInstance<SqlViewTemplate>>,
				getCondationExpr: () => string,
				mode: "left" | "inner",
			}> = []
			const join = <N extends boolean, VT2 extends SqlViewTemplate>(
				mode: "left" | "inner",
				withNull: N,
				view: SqlView<VT2>,
				getCondationExpr: (extra: Relation<N, VT2>, param: SetParam) => string,
			): Relation<N, VT2> => {
				type R = Relation<N, VT2>
				const proxy = proxyInstance(tools, view._getInstance(tools) as RuntimeInstance<R>, (c) => {
					c[sym].withNull ||= withNull
					return c
				})
				extraArr.push({
					mode,
					instance: proxy,
					getCondationExpr: () => getCondationExpr(proxy.template, tools.setParam)
				})
				return proxy.template
			}
			return {
				template: getTemplate(base.template, {
					leftJoin: (withNull, view, getCondation) => join('left', withNull, view, getCondation),
					innerJoin: (view, getCondation) => join('inner', false, view, getCondation),
				}),
				decalerUsedExpr: (expr) => {
					base.decalerUsedExpr(expr)
					extraArr.forEach((e) => e.instance.decalerUsedExpr(expr))
				},
				getSqlBody: (flag) => {
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
						let body = null as null | SelectSqlStruct
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
					return new SelectSqlStruct({
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

	mapTo<const VT extends SqlViewTemplate>(getTemplate: (e: VT1, opts: {
		decalreUsed: (expr: string | Column<boolean, unknown>) => void,
		param: SetParam,
	}) => VT): SqlView<VT> {
		return new SqlView((tools) => {
			const base = this._getInstance(tools)
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

	bracketIf(condation: (sqlBody: SelectSqlStruct) => boolean) {
		return new SqlView((tools) => {
			const instance = proxyInstance<VT1>(tools, this._getInstance(tools), (c) => c)
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
			const instance = proxyInstance<VT1>(tools, this._getInstance(tools), (c) => c)
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
			const instance = this._getInstance(tools)
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
				._getInstance(tools)
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