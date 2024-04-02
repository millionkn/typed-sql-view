import { SqlView } from "./sqlView.js"
import { Column, GetRefStr, InnerColumn, SqlViewTemplate, hasOneOf } from "./tools.js"

export class RawSqlCreator {
  constructor(
    private opts: {
      paramHolder: (index: number) => string,
      skip: 'skip' | 'offset',
      take: 'take' | 'limit',
    }
  ) { }

  selectAll<VT extends { [key: string]: Column<boolean, {}> }>(view: SqlView<VT>) {
    const buildCtx = SqlView.createBuildCtx(view)
    const selectTemplate = buildCtx.template
    const mapper2 = new Map<InnerColumn, string>()
    const formatCbArr = new Array<(raw: { [key: string]: unknown }) => [string, any]>()
    Object.entries(selectTemplate).forEach(([key, column]) => {
      const alias = `value_${mapper2.size}`
      const { withNull, inner, format } = Column.getOpts(column)
      mapper2.set(inner, alias)
      formatCbArr.push((raw) => [key, withNull && raw[alias] === null ? null : format(raw[alias])])
    })
    const sqlBody = buildCtx.analysis({
      order: true,
      usedColumn: [...mapper2.keys()],
    })
    let tableAliasIndex = 0
    let paramIndex = 0
    const params: unknown[] = []
    const sql = sqlBody.build(new Map([...mapper2].map(([inner, alias]) => [inner.resolvable, alias])), {
      sym: {
        skip: this.opts.skip,
        take: this.opts.take,
      },
      resolveSym: () => { throw new Error() },
      genTableAlias: () => `table_${tableAliasIndex++}`,
      setParam: (value) => {
        const holder = this.opts.paramHolder(paramIndex++)
        params.push(value)
        return holder
      },
    })

    return {
      sql,
      params,
      rawFormatter: (raw: { [key: string]: unknown }): {
        [key in keyof VT]: VT[key] extends Column<infer N, infer R> ? ((N extends false ? never : null) | R) : never
      } => {
        return Object.fromEntries(formatCbArr.map((format) => format(raw))) as any
      }
    }
  }

  aggrateView<VT1 extends SqlViewTemplate, VT2 extends { [key: string]: Column<boolean, {}> }>(
    view: SqlView<VT1>,
    getTemplate: (expr: (target: (ref: GetRefStr<VT1>) => string) => Column<boolean, unknown>) => VT2,
  ) {
    return view
      .bracketIf(({ state }) => hasOneOf(state, ['groupBy', 'skip', 'take']))
      .mapTo((e, define) => getTemplate((getTarget) => define((refStr) => getTarget((ref) => refStr(ref(e)))).withNull(true)))
      .pipe((view) => this.selectAll(view))
  }
}