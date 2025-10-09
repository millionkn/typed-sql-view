import { SqlViewTemplate, SelectBodyStruct, Column, Segment, Holder, sql } from "./define.js"
import { SqlView } from "./sqlView.js"
import { exec } from "./tools.js"

export function createSqlView<const VT extends SqlViewTemplate>(
	from: Segment,
	getTemplate: (createColumn: (segment: Segment) => Column<boolean, unknown>, rootAlias: Segment) => VT
) {
	return new SqlView(() => {
		const holder = new Holder(exec(() => {
			const key = {}
			return (helper) => helper.fetchColumnAlias(key)
		}))
		const template = getTemplate((segment) => {
			return new Column({
				withNull: true,
				format: async (raw) => raw,
				builderCtx: segment.createBuilderCtx(),
			})
		}, sql`${holder}`)
		const fromCtx = from.createBuilderCtx()
		return {
			template,
			declareUsed: () => {
				fromCtx.emitInnerUsed()
			},
			snapshot: () => {
				const fromSnapshot = fromCtx.snapshot()
				return {
					getStruct: (flag) => {
						const sqlBody = new SelectBodyStruct({
							from: {
								expr: fromSnapshot.getExpr(),
								alias: holder,
							},
							join: [],
							where: [],
							groupBy: [],
							having: [],
							order: [],
							take: null,
							skip: 0,
						})
						if (!flag.order) { sqlBody.opts.order = [] }
						return sqlBody
					},
				}
			},
		}
	})
}