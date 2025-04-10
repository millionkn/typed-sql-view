import { BuildTools, Column, Relation, SqlBody, SqlViewTemplate, iterateTemplate, hasOneOf, pickConfig, sym } from "./tools.js"

export type BuildFlag = {
	order: boolean,
}


export type RuntimeInstance<VT extends SqlViewTemplate<any>> = {
	template: VT,
	decalerUsedExpr: (expr: string) => void,
	getSqlBody: (flag: BuildFlag) => SqlBody,
}

export type SelectResult<VT extends SqlViewTemplate<any>> = VT extends readonly [] ? []
	: VT extends readonly [infer X extends SqlViewTemplate<any>, ...infer Arr extends readonly SqlViewTemplate<any>[]]
	? [SelectResult<X>, ...SelectResult<Arr>]
	: VT extends readonly (infer X extends SqlViewTemplate<any>)[]
	? SelectResult<X>[]
	: VT extends Column<string, infer N, any, infer R>
	? (true extends N ? null : never) | R
	: VT extends { [key: string]: SqlViewTemplate<any> }
	? { -readonly [key in keyof VT]: SelectResult<VT[key]> }
	: never

export const proxyInstance = <VT extends SqlViewTemplate<any>>(tools: BuildTools, instance: RuntimeInstance<VT>, cb: (column: Column<string, boolean, any, unknown>) => Column<string, boolean, any, unknown>) => {
	const info: Map<string, {
		used: boolean,
		inner: Column<string, boolean, any, unknown>,
		outer: Column<string, boolean, any, unknown>,
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
		decalerUsedExpr: (expr: string) => expr.split(`''""`).forEach((key, i) => {
			if (i % 2 === 0) { return }
			const e = info.get(`''""${key}''""`)
			if (!e) { return }
			if (e.used) { return }
			e.used = true
			instanceUsed = true
			instance.decalerUsedExpr(e.inner[sym].expr)
		}),
		getSqlBody: (opts: {
			flag: BuildFlag,
			bracketIf: (sqlBody: SqlBody) => boolean
		}) => {
			let sqlBody = instance.getSqlBody(opts.flag)
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

export class SqlView<const VT1 extends SqlViewTemplate<any>> {
	constructor(
		private _getInstance: (tools: BuildTools) => RuntimeInstance<VT1>,
	) {
	}

	pipe<R>(op: (self: this) => R): R {
		return op(this)
	}

	buildSelectAll(
		flag: BuildFlag,
		tools: BuildTools,
	) {
		type Ctx = VT1 extends SqlViewTemplate<infer Ctx> ? Ctx : never
		const viewInstance = this._getInstance(tools)
		const selectTarget: Map<string, {
			alias: string,
			format: (raw: { [key: string]: unknown }, ctx: Ctx) => Promise<unknown>,
		}> = new Map()

		iterateTemplate(viewInstance.template, (c) => {
			const opts = c[sym]
			if (selectTarget.has(opts.expr)) { return }
			viewInstance.decalerUsedExpr(opts.expr)
			const alias = `value_${selectTarget.size}`
			if (opts.withNull) {
				selectTarget.set(opts.expr, {
					alias,
					format: async (raw, ctx) => raw[alias] === null ? null : opts.format(raw[alias], ctx),
				})
			} else {
				selectTarget.set(opts.expr, {
					alias,
					format: async (raw, ctx) => opts.format(raw[alias], ctx),
				})
			}

		})
		const rawSql = viewInstance
			.getSqlBody(flag)
			.buildSqlStr([...selectTarget].map(([expr, { alias }]) => ({ expr, alias })))
		return {
			sql: rawSql,
			rawFormatter: async (ctx: VT1 extends SqlViewTemplate<infer Ctx> ? Ctx : never, selectResult: { [key: string]: unknown }) => {
				const loadingArr: Promise<unknown>[] = new Array()
				const resultMapper = new Map<string, unknown>()
				iterateTemplate(viewInstance.template, (c) => {
					const expr = c[sym].expr
					if (!resultMapper.has(expr)) {
						loadingArr.push(selectTarget.get(expr)!.format(selectResult, ctx).then((value) => {
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
		param: {
			(value: unknown): string
			arr: {
				(value: unknown[]): string
			}
		},
		tools: BuildTools,
	) => null | false | undefined | string): SqlView<VT1> {
		return new SqlView((tools) => {
			const instance = proxyInstance<VT1>(tools, this._getInstance(tools), (c) => c)

			return {
				template: instance.template,
				decalerUsedExpr: instance.decalerUsedExpr,
				getSqlBody: (flag) => {
					let condationExpr = getCondation(instance.template, Object.assign((value: unknown) => tools.setParam(value), {
						arr: (value: Iterable<unknown>) => {
							const str = Array.prototype.map.call(value, (v) => tools.setParam(v)).join(',')
							return str.length === 0 ? `(null)` : `(${str})`
						}
					}), tools)
					if (condationExpr) { condationExpr = condationExpr.trim() }
					if (!condationExpr) {
						return instance.getSqlBody({
							flag,
							bracketIf: () => false,
						})
					}
					instance.decalerUsedExpr(condationExpr)
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

	groupBy<const VT extends SqlViewTemplate<any>>(
		getKeyTemplate: (vt: VT1) => SqlViewTemplate<any>,
		getTemplate: (vt: VT1) => VT,
	): SqlView<VT> {
		return new SqlView((tools) => {
			const instance = proxyInstance<VT1>(tools, this._getInstance(tools), (c) => c)
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

	joinLazy<const VT extends SqlViewTemplate<any>>(getTemplate: (e: VT1, opts: {
		leftJoin: <N extends boolean, VT extends SqlViewTemplate<any>>(withNull: N, view: SqlView<VT>, getCondationExpr: (t: Relation<N, VT>) => string) => Relation<N, VT>,
	}) => VT): SqlView<VT> {
		return new SqlView((tools) => {
			const base = proxyInstance<VT1>(tools, this._getInstance(tools), (c) => c)
			const extraArr: Array<{
				instance: ReturnType<typeof proxyInstance<SqlViewTemplate<any>>>,
				getCondationExpr: () => string,
			}> = []
			return {
				template: getTemplate(base.template, {
					leftJoin: (withNull, view, getCondationExpr) => {
						const proxy = proxyInstance<Parameters<typeof getCondationExpr>[0]>(tools, view._getInstance(tools), (c) => {
							c[sym].withNull ||= withNull
							return c
						})
						extraArr.push({
							instance: proxy,
							getCondationExpr: () => getCondationExpr(proxy.template)
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
						let body = null as null | SqlBody
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
					return new SqlBody({
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

	join<const VT extends SqlViewTemplate<any>>(getTemplate: (e: VT1, opts: {
		leftJoin: <N extends boolean, VT extends SqlViewTemplate<any>>(withNull: N, view: SqlView<VT>, getCondationExpr: (t: Relation<N, VT>) => string) => Relation<N, VT>,
		innerJoin: <VT extends SqlViewTemplate<any>>(view: SqlView<VT>, getCondationExpr: (t: VT) => string) => VT,
	}) => VT): SqlView<VT> {
		return new SqlView((tools) => {
			const base = proxyInstance<VT1>(tools, this._getInstance(tools), (c) => c)
			const extraArr: Array<{
				instance: ReturnType<typeof proxyInstance<SqlViewTemplate<any>>>,
				getCondationExpr: () => string,
				mode: "left" | "inner",
			}> = []
			const join = <N extends boolean, VT2 extends SqlViewTemplate<any>>(
				mode: "left" | "inner",
				withNull: N,
				view: SqlView<VT2>,
				getCondationExpr: (extra: Relation<N, VT2>) => string,
			): Relation<N, VT2> => {
				const proxy = proxyInstance<Relation<N, VT2>>(tools, view._getInstance(tools), (c) => {
					c[sym].withNull ||= withNull
					return c
				})
				extraArr.push({
					mode,
					instance: proxy,
					getCondationExpr: () => getCondationExpr(proxy.template)
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
						let body = null as null | SqlBody
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
					return new SqlBody({
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

	mapTo<const VT extends SqlViewTemplate<any>>(getTemplate: (e: VT1, opts: {
		decalreUsed: (expr: string | Column<'', boolean, any, unknown>) => void,
	}) => VT): SqlView<VT> {
		return new SqlView((tools) => {
			const base = this._getInstance(tools)
			return {
				template: getTemplate(base.template, {
					decalreUsed: (expr) => {
						base.decalerUsedExpr(expr.toString())
					},
				}),
				decalerUsedExpr: base.decalerUsedExpr,
				getSqlBody: base.getSqlBody,
			}
		})
	}

	bracketIf(condation: (sqlBody: SqlBody) => boolean) {
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
		getExpr: (template: VT1) => false | null | undefined | string | Column<'', boolean, any, unknown>,
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
					let expr = getExpr(instance.template)?.toString()
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
					const sqlBody = instance.getSqlBody({
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
					const sqlBody = instance.getSqlBody({
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