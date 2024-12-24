import { Inner, exec, hasOneOf, sym, Column, SelectResult, SqlViewTemplate, GetColumnHolder, SqlViewSelectTemplate } from "./define.js";
import { SqlView } from "./sqlView.js";

export class SqlExecutor {
  constructor(
    private opts: {
      skip: (value: number) => string,
      take: (value: number) => string,
      paramHolder: (index: number) => string,
      runner: (sql: string, params: unknown[]) => Promise<{ [key: string]: unknown }[]>
    }
  ) { }

  private rawSelectAll<VT extends SqlViewSelectTemplate>(
    view: SqlView<VT>,
    flags: {
      order: boolean,
    }
  ) {
    const paramArr = [] as unknown[]
    const buildCbArr = [] as Array<(rawSql: string) => string>
    const instance = view.getInstance({
      take: this.opts.take,
      skip: this.opts.skip,
      getAlias: exec(() => {
        let index = 0
        return () => {
          const key1 = `table_${index++}`
          const key2 = `'"${key1}'"`
          buildCbArr.push((raw) => raw.replaceAll(key2, key1))
          return key2
        }
      }),
      setParam: exec(() => {
        let index = 0
        return (value) => {
          const key1 = `'"'"param_${index++}'"'"`
          buildCbArr.push((raw) => {
            if (!raw.includes(key1)) { return raw }
            const key2 = this.opts.paramHolder(paramArr.length)
            paramArr.push(value)
            return raw.replaceAll(key1, key2)
          })
          return key1
        }
      })
    })
    const selectTemplate = instance.template
    const mapper2 = new Map<Inner, string>()
    const formatCbArr = new Array<(baseRaw: unknown, selectResult: { [key: string]: unknown }) => void>()

    const iterateTemplate = (selectTemplate: SqlViewTemplate, accessor: string, path: (baseRaw: unknown) => { [key: string]: unknown }) => {
      if (selectTemplate instanceof Column) {
        const { withNull, inner, format } = selectTemplate[sym](true)
        if (!mapper2.has(inner)) { mapper2.set(inner, `value_${mapper2.size}`) }
        const alias = mapper2.get(inner)!
        if (withNull) {
          formatCbArr.push((baseRaw, selectResult) => {
            const raw = path(baseRaw)
            raw[accessor] = selectResult[alias] === null ? null : format(selectResult[alias])
          })
        } else {
          formatCbArr.push((baseRaw, selectResult) => {
            const raw = path(baseRaw)
            raw[accessor] = format(selectResult[alias])
          })
        }
      } else if (selectTemplate instanceof Array) {
        formatCbArr.push((baseRaw) => {
          const raw = path(baseRaw)
          raw[accessor] ||= []
        })
        selectTemplate.forEach((template, i) => {
          iterateTemplate(template, i.toString(), (baseRaw) => path(baseRaw)[accessor] as { [key: string]: unknown })
        })
      } else {
        formatCbArr.push((baseRaw) => {
          const raw = path(baseRaw)
          raw[accessor] ||= {}
        })
        for (const key in selectTemplate) {
          if (!Object.prototype.hasOwnProperty.call(selectTemplate, key)) { continue }
          const template = selectTemplate[key]
          iterateTemplate(template, key, (baseRaw) => path(baseRaw)[accessor] as { [key: string]: unknown })
        }
      }
    }
    iterateTemplate(selectTemplate, 'result', (raw) => raw as { [key: string]: unknown })
    const sqlBody = instance.getSelectBody({
      order: flags.order,
      usedInner: [...mapper2.keys()]
    })
    let rawSql = sqlBody.buildSql([...mapper2].map(([inner, alias]) => {
      return {
        select: inner.expr,
        alias,
      }
    }))
    buildCbArr.forEach((cb) => {
      rawSql = cb(rawSql)
    })

    return {
      sql: rawSql,
      paramArr: paramArr,
      rawFormatter: (selectResult: { [key: string]: unknown }): SelectResult<VT> => {
        const raw = { result: {} as any }
        formatCbArr.forEach((effect) => effect(raw, selectResult))
        return raw.result
      }
    }
  }


  aggrateView<VT1 extends SqlViewTemplate, VT2 extends SqlViewSelectTemplate>(
    view: SqlView<VT1>,
    getTemplate: (expr: (target: (ref: GetColumnHolder<VT1>) => string) => Column<boolean, unknown>) => VT2,
  ) {
    return view
      .bracketIf(({ state }) => hasOneOf(state, ['groupBy', 'skip', 'take']))
      .mapTo((e, define) => getTemplate((getTarget) => define((refStr) => getTarget((ref) => refStr(ref(e)))).withNull(true)))
      .pipe(async (view) => {
        const rawSelect = this.rawSelectAll(view, { order: false })
        return this.opts.runner(rawSelect.sql, rawSelect.paramArr).then(([raw]) => {
          if (!raw) { throw new Error('aggrate no result') }
          return rawSelect.rawFormatter(raw)
        })
      })
  }

  async selectAll<VT extends SqlViewSelectTemplate>(view: SqlView<VT>) {
    const rawSql = this.rawSelectAll(view, {
      order: true,
    })
    return this.opts.runner(rawSql.sql, rawSql.paramArr).then((arr) => arr.map((raw) => rawSql.rawFormatter(raw)))
  }

  async selectOne<VT extends SqlViewSelectTemplate>(view: SqlView<VT>) {
    return this.selectAll(view.take(1)).then((arr) => arr.at(0) ?? null)
  }



  async getTotal(view: SqlView<SqlViewTemplate>) {
    return this.aggrateView(view, (expr) => {
      return { count: expr(() => `count(*)`).format((raw) => Number(raw)).withNull(false) }
    }).then((e) => e.count)
  }

  async query<VT extends SqlViewSelectTemplate>(withCount: boolean, page: null | { take: number, skip: number }, view: SqlView<VT>) {
    return Promise.all([
      this.selectAll(view.skip(page?.skip).take(page?.take)),
      withCount ? this.getTotal(view) : -1,
    ]).then(([data, total]) => ({ data, total }))
  }
}

