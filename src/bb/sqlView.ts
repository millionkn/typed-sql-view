import { BuildCtx, Column, Relation, SqlBody, SqlViewTemplate, iterateTemplate, hasOneOf, pickConfig, sym } from "./tools.js"

export type BuildFlag = {
	order: boolean,
}


export type RuntimeInstance<VT extends SqlViewTemplate<string>> = {
	template: VT,
	decalerUsedExpr: (expr: string) => void,
	getSqlBody: (flag: BuildFlag) => SqlBody,
}

export type SelectResult<VT extends SqlViewTemplate<''>> = VT extends readonly [] ? []
	: VT extends readonly [infer X extends SqlViewTemplate<''>, ...infer Arr extends readonly SqlViewTemplate<''>[]]
	? [SelectResult<X>, ...SelectResult<Arr>]
	: VT extends readonly (infer X extends SqlViewTemplate<''>)[]
	? SelectResult<X>[]
	: VT extends Column<infer X, infer Y>
	? (true extends X ? null : never) | Y
	: VT extends { [key: string]: SqlViewTemplate<''> }
	? { -readonly [key in keyof VT]: SelectResult<VT[key]> }
	: never

export const proxyInstance = <VT extends SqlViewTemplate<string>>(ctx: BuildCtx, instance: RuntimeInstance<SqlViewTemplate<string>>, cb: (column: Column<string>) => Column<string>) => {
	const info: Map<string, {
		used: boolean,
		inner: Column<string>,
		outer: Column<string>,
		replaceWith: (expr: string) => void,
	}> = new Map()
	let instanceUsed = false
	return {
		instanceUsed: () => instanceUsed,
		info,
		template: iterateTemplate(instance.template, (inner) => {
			const outer = cb(inner.withNull(inner[sym].withNull))
			const holder = ctx.createHolder()
			outer[sym].expr = holder.expr
			info.set(holder.expr, {
				used: false,
				inner,
				outer,
				replaceWith: holder.replaceWith,
			})
			return outer
		}) as VT,
		decalerUsedExpr: (expr: string) => expr.split(`'"'"`).forEach((key, i) => {
			if (i % 2 === 0) { return }
			const e = info.get(key)
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
			const tableAlias = ctx.genAlias()
			const selectTarget = [] as {
				expr: string,
				alias: string,
			}[]
			info.forEach((v) => {
				if (!v.used) { return }
				const alias = `${tableAlias}.value_${selectTarget.length + 1}`
				v.replaceWith(alias)
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

export class SqlView<VT1 extends SqlViewTemplate<string>> {
	constructor(
		private _getInstance: (buildCtx: BuildCtx) => RuntimeInstance<VT1>,
	) {
	}

	pipe<R>(op: (self: this) => R): R {
		return op(this)
	}

	static buildSelectAll<VT extends SqlViewTemplate<''>>(
		view: SqlView<VT>,
		flag: BuildFlag,
		ctx: BuildCtx,
	) {
		const viewInstance = view._getInstance(ctx)
		const selectTarget: Map<string, {
			alias: string,
			format: (raw: { [key: string]: unknown }) => unknown,
		}> = new Map()

		iterateTemplate(viewInstance.template, (c) => {
			const opts = c[sym]
			if (selectTarget.has(opts.expr)) { return }
			const alias = `value_${selectTarget.size}`
			if (opts.withNull) {
				selectTarget.set(opts.expr, {
					alias,
					format: (raw) => raw[alias] === null ? null : opts.format(raw[alias]),
				})
			} else {
				selectTarget.set(opts.expr, {
					alias,
					format: opts.format,
				})
			}

		})
		const rawSql = viewInstance
			.getSqlBody(flag)
			.buildSqlStr([...selectTarget].map(([expr, { alias }]) => ({ expr, alias })))
		return {
			sql: rawSql,
			rawFormatter: (selectResult: { [key: string]: unknown }) => {
				return iterateTemplate(viewInstance.template, (c) => selectTarget.get(c[sym].expr)!.format(selectResult)) as SelectResult<VT>
			}
		}
	}

	andWhere(getCondation: (
		template: VT1,
		param: (value: any) => string,
	) => null | false | undefined | string): SqlView<VT1> {
		return new SqlView((ctx) => {
			const instance = proxyInstance<VT1>(ctx, this._getInstance(ctx), (c) => c)

			return {
				template: instance.template,
				decalerUsedExpr: instance.decalerUsedExpr,
				getSqlBody: (flag) => {
					let condationExpr = getCondation(instance.template, (value) => {
						if (value instanceof Array) {
							return `(${value.map((v) => ctx.setParam(v)).join(',')})`
						} else {
							return ctx.setParam(value)
						}
					})
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

	groupBy<K extends SqlViewTemplate<''>, VT extends SqlViewTemplate<string>>(
		getKeyTemplate: (vt: VT1) => K,
		getValueTemplate: (template: VT1) => VT,
	): SqlView<{ keys: K, content: VT }> {
		return new SqlView((ctx) => {
			const instance = proxyInstance<VT1>(ctx, this._getInstance(ctx), (c) => c)
			const keys = getKeyTemplate(instance.template)
			const content = getValueTemplate(instance.template)
			return {
				template: { keys, content },
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

	joinUnstable<VT extends SqlViewTemplate<string>>(getTemplate: (e: VT1, opts: {
		lazyJoin: <N extends boolean, VT extends SqlViewTemplate<string>>(withNull: N, view: SqlView<VT>, getCondationExpr: (t: Relation<N, VT>) => string) => Relation<N, VT>,
		leftJoin: <N extends boolean, VT extends SqlViewTemplate<string>>(withNull: N, view: SqlView<VT>, getCondationExpr: (t: Relation<N, VT>) => string) => Relation<N, VT>,
		innerJoin: <VT extends SqlViewTemplate<string>>(view: SqlView<VT>, getCondationExpr: (t: VT) => string) => VT,
	}) => VT): SqlView<VT> {
		return new SqlView((ctx) => {
			const base = proxyInstance<VT1>(ctx, this._getInstance(ctx), (c) => c)
			const extraArr: Array<{
				instance: ReturnType<typeof proxyInstance<SqlViewTemplate<string>>>,
				getCondationExpr: () => string,
				mode: "left" | "inner" | "lazy",
			}> = []
			const join = <N extends boolean, VT2 extends SqlViewTemplate<string>>(
				mode: "left" | "inner" | "lazy",
				withNull: N,
				view: SqlView<VT2>,
				getCondationExpr: (extra: Relation<N, VT2>) => string,
			): Relation<N, VT2> => {
				const proxy = proxyInstance<Relation<N, VT2>>(ctx, view._getInstance(ctx), (c) => {
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
					lazyJoin: (withNull, view, getCondation) => join('lazy', withNull, view, getCondation),
					leftJoin: (withNull, view, getCondation) => join('left', withNull, view, getCondation),
					innerJoin: (view, getCondation) => join('inner', false, view, getCondation),
				}),
				decalerUsedExpr: (expr) => {
					base.decalerUsedExpr(expr)
					extraArr.forEach((e) => e.instance.decalerUsedExpr(expr))
				},
				getSqlBody: (flag) => {
					const usedExtraArr = extraArr.filter((e) => {
						if (e.mode === 'lazy') {
							return e.instance.instanceUsed()
						}
						return true
					})
					if (usedExtraArr.length === 0) {
						base.info.forEach((e) => {
							e.replaceWith(e.inner[sym].expr)
						})
						return base.getSqlBody({ flag, bracketIf: () => false })
					}
					const arr = usedExtraArr.map(({ getCondationExpr, instance, mode }) => {
						const condationExpr = getCondationExpr()
						base.decalerUsedExpr(condationExpr)
						instance.decalerUsedExpr(condationExpr)
						return {
							condationExpr,
							mode,
							body: instance.getSqlBody({
								flag: {
									order: pickConfig(mode, {
										'lazy': () => false,
										'left': () => flag.order,
										'inner': () => flag.order,
									}),
								},
								bracketIf: (sqlBody) => pickConfig(mode, {
									lazy: () => hasOneOf(sqlBody.state(), ['leftJoin', 'innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
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
							...arr.flatMap(({ body, mode, condationExpr }) => {
								return [
									...body.opts.from.map((info, index) => {
										return {
											type: index === 0 ? 'inner' as const : pickConfig(mode, {
												left: () => 'left' as const,
												inner: () => 'inner' as const,
												lazy: () => 'left' as const,
											}),
											alias: info.alias,
											expr: info.expr,
											condation: condationExpr,
										}
									}),
									...body.opts.join,
								]
							}),
						],
						where: [
							...baseBody.opts.where ?? [],
							...arr.flatMap(({ body }) => {
								return body.opts.where
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

	mapTo<VT extends SqlViewTemplate<string>>(getTemplate: (e: VT1, opts: {
		decalreUsed: (expr: string) => void,
	}) => VT): SqlView<VT> {
		return new SqlView((ctx) => {
			const base = this._getInstance(ctx)
			return {
				template: getTemplate(base.template, {
					decalreUsed: (expr) => {
						base.decalerUsedExpr(expr)
					},
				}),
				decalerUsedExpr: base.decalerUsedExpr,
				getSqlBody: base.getSqlBody,
			}
		})
	}

	bracketIf(condation: (sqlBody: SqlBody) => boolean) {
		return new SqlView((ctx) => {
			const instance = proxyInstance<VT1>(ctx, this._getInstance(ctx), (c) => c)
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
		getExpr: (template: VT1) => false | null | undefined | string,
	): SqlView<VT1> {
		return new SqlView((ctx) => {
			const instance = proxyInstance<VT1>(ctx, this._getInstance(ctx), (c) => c)
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
					let expr = getExpr(instance.template)
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
		return new SqlView((ctx) => {
			const instance = this._getInstance(ctx)
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
		return new SqlView((ctx) => {
			const instance = this
				.bracketIf((sqlBody) => hasOneOf(sqlBody.state(), ['take']))
				._getInstance(ctx)
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