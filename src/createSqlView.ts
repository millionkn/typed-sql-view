import { SqlViewTemplate, SqlBody } from "./tools.js"
import { SqlView } from "./sqlView.js"

export function createSqlView<const VT extends SqlViewTemplate<any>>(
	getTemplate: (opts: {
		param: {
			(value: any): string
			arr: (value: any[]) => string
		},
		addFrom: (expr: string) => string,
		leftJoin: (expr: string, condation: (alias: string) => string) => string,
		innerJoin: (expr: string, condation: (alias: string) => string) => string,
		andWhere: (condation: string) => void,
		addOrder: (order: 'asc' | 'desc', expr: string) => void,
	}) => VT
) {
	return new SqlView((ctx) => {
		const sqlBody = new SqlBody({
			from: [],
			join: [],
			where: [],
			groupBy: [],
			having: [],
			order: [],
			take: null,
			skip: 0,
		})
		const template = getTemplate({
			param: Object.assign((value: unknown) => ctx.setParam(value), {
				arr: (value: Iterable<unknown>) => {
					const str = Array.prototype.map.call(value, (v) => ctx.setParam(v)).join(',')
					return str.length === 0 ? `(null)` : `(${str})`
				}
			}),
			addFrom: (expr) => {
				const alias = ctx.genAlias()
				sqlBody.opts.from.push({
					alias,
					expr,
				})
				return alias
			},
			andWhere: (c) => sqlBody.opts.where.push(c),
			innerJoin: (raw, condation) => {
				const alias = ctx.genAlias()
				sqlBody.opts.join.push({
					type: 'inner',
					alias,
					expr: raw,
					condation: condation(alias),
				})
				return alias
			},
			leftJoin: (raw, condation) => {
				const alias = ctx.genAlias()
				sqlBody.opts.join.push({
					type: 'left',
					alias,
					expr: raw,
					condation: condation(alias),
				})
				return alias
			},
			addOrder: (order, expr) => {
				sqlBody.opts.order.push({ order, expr })
			}
		})
		return {
			template,
			decalerUsedExpr: (expr) => { },
			getSqlBody: ({ order }) => {
				if (!order) { sqlBody.opts.order = [] }
				return sqlBody
			},
		}
	})
}