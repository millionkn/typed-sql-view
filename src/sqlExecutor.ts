import { Column, GetColumnHolder, SqlViewTemplate } from "./define.js";
import { Adapter } from "./adapter.js";
import { SqlView } from "./sqlView.js";

export class SqlExecutor<P> {
  constructor(
    private opts: {
      adapter: Adapter<P>,
      runner: (sql: string, params: P) => Promise<{ [key: string]: unknown }[]>
    }
  ) { }

  async selectAll<VT extends SqlViewTemplate>(view: SqlView<VT>) {
    const rawSql = this.opts.adapter.selectAll(view)
    return this.opts.runner(rawSql.sql, rawSql.param).then((arr) => arr.map((raw) => rawSql.rawFormatter(raw)))
  }

  async selectOne<VT extends SqlViewTemplate>(view: SqlView<VT>) {
    return this.selectAll(view.take(1)).then((arr) => arr.at(0) ?? null)
  }

  async aggrateView<VT1 extends SqlViewTemplate, VT2 extends SqlViewTemplate>(
    view: SqlView<VT1>,
    getTemplate: (expr: (target: (ref: GetColumnHolder<VT1>) => string) => Column<boolean, unknown>) => VT2,
  ) {
    const rawSelect = this.opts.adapter.aggrateView(view, getTemplate)
    return this.opts.runner(rawSelect.sql, rawSelect.param).then(([raw]) => {
      if (!raw) { throw new Error('aggrate no result') }
      return rawSelect.rawFormatter(raw)
    })
  }

  async getTotal(view: SqlView<SqlViewTemplate>) {
    return this.aggrateView(view, (expr) => {
      return { count: expr(() => `count(*)`).format((raw) => Number(raw)).withNull(false) }
    }).then((e) => e.count)
  }

  async query<VT extends SqlViewTemplate>(withCount: boolean, page: null | { take: number, skip: number }, view: SqlView<VT>) {
    return Promise.all([
      this.selectAll(view.skip(page?.skip).take(page?.take)),
      withCount ? this.getTotal(view) : -1,
    ]).then(([data, total]) => ({ data, total }))
  }
}

