import { SqlViewTemplate, SelectSqlStruct, SqlSegment } from "./define.js"
import { SqlView } from "./sqlView.js"

export function createSqlView<const VT extends SqlViewTemplate>(
	from: SqlSegment,
	getTemplate: (rootAlias: SqlSegment) => VT
) {

	return new SqlView(() => {
		const rootAlias = ctx.genAlias()
		const template = getTemplate(rootAlias)
		const sqlBody = new SelectSqlStruct({
			from: [{
				alias: rootAlias,
				expr: from,
			}],
			join: [],
			where: [],
			groupBy: [],
			having: [],
			order: [],
			take: null,
			skip: 0,
		})

		return {
			template,
			getSqlStruct: ({ order }) => {
				if (!order) { sqlBody.opts.order = [] }
				return sqlBody
			},
		}
	})
}