import { exec, sym, Column, SelectResult, SqlViewTemplate, SqlAdapter, GetColumnRef, CreateResolver, Inner } from "./tools.js";
import { BuildFlag, SqlView } from "./sqlView.js";

export class SqlExecutor {
  constructor(
    private opts: {
      adapter: SqlAdapter,
      runner: (sql: string, params: unknown[]) => Promise<{ [key: string]: unknown }[]>
    }
  ) { }

  private rawSelectAll<VT extends SqlViewTemplate<''>>(
    view: SqlView<VT>,
    flag: BuildFlag,
  ) {
    const createResolver: CreateResolver = exec(() => {
      let _nsIndex = 0
      return <V>() => {
        const nsIndex = _nsIndex++
        let index = 0
        const saved = new Map<string, () => V>()
        return {
          createHolder: (getValue: () => V) => {
            const key = `holder_${nsIndex}_${index++}`
            saved.set(key, getValue)
            return `'"'"${key}'"'"`
          },
          resolve: (str: string) => {
            return str.split(`'"'"`).map((str, i) => {
              if (i % 2 === 0) { return str }
              const getValue = saved.get(str)
              if (!getValue) { return `'"'"${str}'"'"` }
              return getValue
            })
          }
        }
      }
    })

    const paramArr = [] as unknown[]
    const resolver = createResolver<string>()
    const viewResult = view.rawBuild(flag, {
      adapter: this.opts.adapter, 
      createResolver,
      genAlias: exec(() => {
        let index = 0
        return () => resolver.createHolder(() => `table_${index++}`)
      }),
      setParam: exec(() => {
        let index = 0
        return (value) => resolver.createHolder(() => {
          paramArr[index] = value
          return this.opts.adapter.paramHolder(index++)
        })
      }),
    })
    const selectTarget = new Map<Inner, string>()
    const formatCbArr = new Array<(baseRaw: unknown, selectResult: { [key: string]: unknown }) => void>()
    const iterateTemplate = (selectTemplate: SqlViewTemplate<''>, accessor: string, path: (baseRaw: unknown) => { [key: string]: unknown }) => {
      if (selectTemplate instanceof Column) {
        const { withNull, inner, format } = selectTemplate[sym] 
        if (!selectTarget.has(inner)) { selectTarget.set(inner, `value_${selectTarget.size}`) }
        const alias = selectTarget.get(inner)!
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
    iterateTemplate(viewResult.template, 'result', (raw) => raw as { [key: string]: unknown })

    const selectTargetStr = [...selectTarget]
      .map(([inner, alias]) => `${inner.expr} ${alias}`)
      .join(',') ?? '1'
    const rawSql = [
      `select ${resolver.resolve(selectTargetStr).map((e) => typeof e === 'string' ? e : e())}`,
      !viewResult.source ? '' : `from`,
      resolver.resolve(viewResult.source).map((e) => typeof e === 'string' ? e : e()),
    ].join(' ')

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


  // aggrateView<VT1 extends SqlViewTemplate<string>, VT2 extends SqlViewTemplate<''>>(
  //   view: SqlView<VT1>,
  //   getTemplate: (column: (getExpr: (getRef: GetColumnRef<VT1>) => string) => Column<''>) => VT2,
  // ) {
  //   return view
  //     .bracketIf(({ state }) => hasOneOf(state, ['groupBy', 'skip', 'take']))
  //     .mapTo((e, define) => getTemplate((getTarget) => define((refStr) => getTarget((ref) => refStr(ref(e)))).withNull(true)))
  //     .pipe(async (view) => {
  //       const rawSelect = this.rawSelectAll(view, { order: false })
  //       return this.opts.runner(rawSelect.sql, rawSelect.paramArr).then(([raw]) => {
  //         if (!raw) { throw new Error('aggrate no result') }
  //         return rawSelect.rawFormatter(raw)
  //       })
  //     })
  // }

  async selectAll<VT extends SqlViewTemplate<''>>(view: SqlView<VT>) {
    const rawSql = this.rawSelectAll(view, {
      order: true,
    })
    return this.opts.runner(rawSql.sql, rawSql.paramArr).then((arr) => arr.map((raw) => rawSql.rawFormatter(raw)))
  }

  // async selectOne<VT extends SqlViewTemplate<''>>(view: SqlView<VT>) {
  //   return this.selectAll(view.take(1)).then((arr) => arr.at(0) ?? null)
  // }



  // async getTotal(view: SqlView<SqlViewTemplate<string>>) {
  //   return this.aggrateView(view, (expr) => {
  //     return { count: expr(() => `count(*)`).format((raw) => Number(raw)).withNull(false) }
  //   }).then((e) => e.count)
  // }

  // async query<VT extends SqlViewTemplate<''>>(withCount: boolean, page: null | { take: number, skip: number }, view: SqlView<VT>) {
  //   return Promise.all([
  //     this.selectAll(view.skip(page?.skip).take(page?.take)),
  //     withCount ? this.getTotal(view) : -1,
  //   ]).then(([data, total]) => ({ data, total }))
  // }
}

