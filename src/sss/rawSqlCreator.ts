import { SqlView, createInitContext, createTransformContext } from "./sqlView.js"
import { Adapter, AnalysisResult, Column, ExprResolver, GetRefStr, SqlViewTemplate, hasOneOf, privateSym } from "./tools.js"

export class RawSqlCreator {
  constructor(
    private adapter: Adapter
  ) { }

  private rawSelectAll<VT extends { [key: string]: Column<boolean, {} | null> }>(
    view: SqlView<VT>,
    analysisResult: AnalysisResult,
  ) {
    const initCtx = createInitContext()
    const buildCtx = this.adapter.createBuildCtx()
    const transformCtx = createTransformContext(view, initCtx)
    const selectTemplate = transformCtx.template
    const mapper2 = new Map<ExprResolver, string>()
    const formatCbArr = new Array<(raw: { [key: string]: unknown }) => [string, any]>()
    for (const key in selectTemplate) {
      if (!Object.prototype.hasOwnProperty.call(selectTemplate, key)) { continue }
      const column = selectTemplate[key];
      const { withNull, inner: { exprResolver }, format } = Column[privateSym].getOpts(column)
      transformCtx.appendUsedColumn(column)
      if (!mapper2.has(exprResolver)) { mapper2.set(exprResolver, `value_${mapper2.size}`) }
      const alias = mapper2.get(exprResolver)!
      formatCbArr.push((raw) => [key, withNull && raw[alias] === null ? null : format(raw[alias])])
    }
    const sqlBody = transformCtx.analysis(analysisResult)
    const sql = sqlBody.build(mapper2, buildCtx)

    return {
      sql,
      rawFormatter: (raw: { [key: string]: unknown }): {
        -readonly [key in keyof VT]: VT[key] extends Column<infer N, infer R> ? ((N extends false ? never : null) | R) : never
      } => {
        return Object.fromEntries(formatCbArr.map((format) => format(raw))) as any
      }
    }
  }

  selectAll<VT extends { [key: string]: Column<boolean, {} | null> }>(view: SqlView<VT>) {
    return this.rawSelectAll(view, {
      order: true,
    })
  }

  aggrateView<VT1 extends SqlViewTemplate, VT2 extends { [key: string]: Column<boolean, {} | null> }>(
    view: SqlView<VT1>,
    getTemplate: (expr: (target: (ref: GetRefStr<VT1>) => string) => Column<boolean, unknown>) => VT2,
  ) {
    return view
      .bracketIf(({ state }) => hasOneOf(state, ['groupBy', 'skip', 'take']))
      .mapTo((e, define) => getTemplate((getTarget) => define((refStr) => getTarget((ref) => refStr(ref(e)))).withNull(true)))
      .pipe((view) => this.rawSelectAll(view, { order: false }))
  }
}