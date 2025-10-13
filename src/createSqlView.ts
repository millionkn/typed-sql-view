import { SqlViewTemplate, SelectBodyStruct, Column, Segment, Holder } from "./define.js"
import { SqlView } from "./sqlView.js"
import { exec } from "./tools.js"

export function createSqlView<const VT extends SqlViewTemplate>(
	from: Segment,
	getTemplate: (createColumn: (getSegemnt: (rootAlias: Segment) => Segment) => Column<boolean, unknown>) => VT
) {
	return new SqlView(() => {
		const holder = new Holder(exec(() => {
			const key = Symbol('rootAlias')
			return (helper) => helper.fetchTableAlias(key)
		}))
		const rootAlias = new Segment(() => {
			return {
				emitInnerUsed: () => { },
				buildExpr: () => [holder],
			}
		})
		const template = getTemplate((getSegment) => {
			return new Column({
				withNull: true,
				format: async (raw) => raw,
				builderCtx: getSegment(rootAlias).createBuilderCtx(),
			})
		})
		const builderCtx = from.createBuilderCtx()
		return {
			template,
			emitInnerUsed: () => {
				builderCtx.emitInnerUsed()
			},
			finalize: (flag) => {
				const sqlBody = new SelectBodyStruct({
					from: {
						expr: builderCtx.buildExpr(),
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
	})
}