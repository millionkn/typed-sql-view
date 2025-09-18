import { SqlViewTemplate, SelectSqlStruct, SqlSegment, sym2 } from "./define.js"
import { SqlView } from "./sqlView.js"

export function createSqlView<const VT extends SqlViewTemplate>(
	from: SqlSegment,
	getTemplate: (rootAlias: SqlSegment) => VT
) {
	return new SqlView((ctx) => {
		const rootAlias = ctx.genAlias()

		const sqlBody = new SelectSqlStruct({
			from: [{
				alias: rootAlias,
				expr: from[sym2](),
			}],
			join: [],
			where: [],
			groupBy: [],
			having: [],
			order: [],
			take: null,
			skip: 0,
		})
		const template = getTemplate(rootAlias)
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