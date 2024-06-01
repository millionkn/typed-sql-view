import { Column, GetColumnHolder, Inner, SqlViewTemplate } from "./define.js"
import { SqlView } from "./sqlView.js"
import { exec, hasOneOf, privateSym } from './private.js'
import { resolveSqlStr } from "./tools.js"

export class Adapter<PC = unknown> {
  static mysqlAdapter = new Adapter({
    language: {
      skip: 'skip',
      take: 'take',
    },
    createParamCtx: () => {
      const result: { [key: string]: unknown } = {}
      return {
        getParamResult: () => result,
        setParam: (value, index) => {
          result[`param${index}`] = value
          return { holder: `:param${index}` }
        },
      }
    },
  })

  static postgresAdapter = new Adapter({
    language: {
      skip: 'offset',
      take: 'limit',
    },
    createParamCtx: () => {
      const result: unknown[] = []
      return {
        getParamResult: () => result,
        setParam: (value, index) => {
          result.push(value)
          return { holder: `$${index + 1}` }
        },
      }
    },
  })

  constructor(public opts: {
    createParamCtx: () => {
      getParamResult: () => PC,
      setParam: (value: unknown, index: number) => { holder: string }
    },
    language: {
      skip: 'skip' | 'offset',
      take: 'take' | 'limit',
    }
  }) { }
}

export class RawSqlCreator<P> {
  constructor(
    private adapter: Adapter<P>
  ) { }

  private rawSelectAll<VT extends { [key: string]: Column<boolean, {} | null> }>(
    view: SqlView<VT>,
    flags: {
      order: boolean,
    }
  ) {
    const param = exec(() => {
      let index = 0
      const paramCtx = this.adapter.opts.createParamCtx()
      return {
        getResult: () => paramCtx.getParamResult(),
        set: (v: any) => paramCtx.setParam(v, index++).holder
      }
    })
    const genTableAlias = exec(() => {
      let index = 0
      return () => `table_${index++}`
    })

    const instance = view.getInstance({
      language: this.adapter.opts.language,
      genTableAlias,
      setParam: (v) => param.set(v),
      createColumnHelper: () => {
        const saved = new Map<Inner, Inner[]>()
        return {
          getDeps: (inner) => saved.get(inner) ?? null,
          createColumn: (getExpr) => {
            const expr = resolveSqlStr<Inner>((holder) => getExpr((inner) => holder(inner)))
            const inner: Inner = {
              [privateSym]: 'inner',
              segment: expr.flatMap((e) => typeof e === 'string' ? e : e.segment)
            }
            saved.set(inner, [...new Set(expr.flatMap((e) => typeof e === 'string' ? [] : saved.get(e) ?? e))])
            return Column[privateSym](inner)
          },
        }
      },
    })
    const selectTemplate = instance.template
    const mapper2 = new Map<Inner, string>()
    const formatCbArr = new Array<(raw: { [key: string]: unknown }) => [string, any]>()
    for (const key in selectTemplate) {
      if (!Object.prototype.hasOwnProperty.call(selectTemplate, key)) { continue }
      const column = selectTemplate[key];
      const { withNull, inner, format } = column.opts
      if (!mapper2.has(inner)) { mapper2.set(inner, `value_${mapper2.size}`) }
      const alias = mapper2.get(inner)!
      formatCbArr.push((raw) => [key, withNull && raw[alias] === null ? null : format(raw[alias])])
    }
    const sqlBody = instance.getSqlBody({
      order: flags.order,
      usedInner: [...mapper2.keys()]
    })
    const sql = sqlBody.build(
      new Map([...mapper2.entries()].map(([inner, alias]) => [inner.segment, alias])),
      { resolveAliasSym: () => { throw new Error() } },
    )

    return {
      sql,
      param: param.getResult(),
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
    getTemplate: (expr: (target: (ref: GetColumnHolder<VT1>) => string) => Column<boolean, unknown>) => VT2,
  ) {
    return view
      .bracketIf(({ state }) => hasOneOf(state, ['groupBy', 'skip', 'take']))
      .mapTo((e, define) => getTemplate((getTarget) => define((refStr) => getTarget((ref) => refStr(ref(e)))).withNull(true)))
      .pipe((view) => this.rawSelectAll(view, { order: false }))
  }
}