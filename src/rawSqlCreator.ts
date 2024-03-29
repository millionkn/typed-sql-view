import { SqlView } from "./sqlView.js"
import { Column, GetRefStr, SqlViewTemplate, hasOneOf } from "./tools.js"

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
    const usedColumn = [...new Set(Object.values(selectTemplate))]
    const sqlBody = buildCtx.analysis({
      order: true,
      usedColumn,
    })
    let tableAliasIndex = 0
    let paramIndex = 0
    const params: unknown[] = []
    const sql = sqlBody.build(new Map(usedColumn.map((c, i) => [Column.getOpts(c).inner.resolvable, `value_${i}`])), {
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
        return Object.fromEntries(Object.entries(selectTemplate).map(([key, column]) => {
          const opts = Column.getOpts(column)
          const index = usedColumn.findIndex((c) => c === column)
          if (opts.withNull) {
            return [key, raw[`value_${index}`] === null ? null : opts.format(raw[`value_${index}`])]
          } else {
            return [key, opts.format(raw[`value_${index}`])]
          }
        })) as any
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