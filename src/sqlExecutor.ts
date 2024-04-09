import { RawSqlCreator } from "./rawSqlCreator.js";
import { SqlView } from "./sqlView.js";
import { Column, GetRefStr, SqlViewTemplate } from "./tools.js";

export class SqlExecutor {
  constructor(
    private opts: {
      creator: RawSqlCreator,
      runner: (sql: string, params: unknown[]) => Promise<{ [key: string]: unknown }[]>
    }
  ) { }

  async selectAll<VT extends { [key: string]: Column<boolean, {}> }>(view: SqlView<VT>) {
    const rawSql = this.opts.creator.selectAll(view)
    return this.opts.runner(rawSql.sql, rawSql.params).then((arr) => arr.map((raw) => rawSql.rawFormatter(raw)))
  }

  async selectOne<VT extends { [key: string]: Column<boolean, {}> }>(view: SqlView<VT>) {
    return this.selectAll(view.take(1)).then((arr) => arr.at(0) ?? null)
  }

  async aggrateView<VT1 extends SqlViewTemplate, VT2 extends { [key: string]: Column<boolean, {}> }>(
    view: SqlView<VT1>,
    getTemplate: (expr: (target: (ref: GetRefStr<VT1>) => string) => Column<boolean, unknown>) => VT2,
  ) {
    const rawSelect = this.opts.creator.aggrateView(view, getTemplate)
    return this.opts.runner(rawSelect.sql, rawSelect.params).then(([raw]) => {
      if (!raw) { throw new Error('aggrate no result') }
      return rawSelect.rawFormatter(raw)
    })
  }

  async getTotal(view: SqlView<SqlViewTemplate>) {
    return this.aggrateView(view, (expr) => {
      return { count: expr(() => `count(*)`).format((raw) => Number(raw)).withNull(false) }
    }).then((e) => e.count)
  }

  async selectList<VT extends { [key: string]: Column<boolean, {}> }>(withCount: boolean, take: number, skip: number, view: SqlView<VT>) {
    return Promise.all([
      this.selectAll(view.take(take).skip(skip)),
      withCount ? this.getTotal(view) : -1,
    ])
  }
}

