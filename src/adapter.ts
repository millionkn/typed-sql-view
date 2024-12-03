import { Column, GetColumnHolder, Inner, SelectResult, SqlViewTemplate } from "./define.js"
import { SqlView } from "./sqlView.js"
import { exec, hasOneOf } from './private.js'

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

  constructor(
    private opts: {
      createParamCtx: () => {
        getParamResult: () => PC,
        setParam: (value: unknown, index: number) => { holder: string }
      }
      language: {
        skip: 'skip' | 'offset',
        take: 'take' | 'limit',
      }
    }
  ) { }

  private rawSelectAll<VT extends SqlViewTemplate>(
    view: SqlView<VT>,
    flags: {
      order: boolean,
    }
  ) {
    const param = exec(() => {
      let index = 0
      const paramCtx = this.opts.createParamCtx()
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
      language: this.opts.language,
      genTableAlias,
      setParam: (v) => param.set(v),
    })
    const selectTemplate = instance.template
    const mapper2 = new Map<Inner, string>()
    const effectCbArr = new Array<(baseRaw: unknown, selectResult: { [key: string]: unknown }) => void>()

    const iterateTemplate = (selectTemplate: SqlViewTemplate, accessor: string, path: (baseRaw: unknown) => { [key: string]: unknown }) => {
      if (selectTemplate instanceof Column) {
        const { withNull, inner, format } = selectTemplate.opts
        if (!mapper2.has(inner)) { mapper2.set(inner, `value_${mapper2.size}`) }
        const alias = mapper2.get(inner)!
        if (withNull) {
          effectCbArr.push((baseRaw, selectResult) => {
            const raw = path(baseRaw)
            raw[accessor] = selectResult[alias] === null ? null : format(selectResult[alias])
          })
        } else {
          effectCbArr.push((baseRaw, selectResult) => {
            const raw = path(baseRaw)
            raw[accessor] = format(selectResult[alias])
          })
        }
      } else if (selectTemplate instanceof Array) {
        effectCbArr.push((baseRaw) => {
          const raw = path(baseRaw)
          raw[accessor] ||= []
        })
        selectTemplate.forEach((template, i) => {
          iterateTemplate(template, i.toString(), (baseRaw) => path(baseRaw)[accessor] as { [key: string]: unknown })
        })
      } else {
        effectCbArr.push((baseRaw) => {
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
    const sqlBody = instance.getSqlBody({
      order: flags.order,
      usedInner: [...mapper2.keys()]
    })
    const sql = sqlBody.build(mapper2)

    return {
      sql,
      param: param.getResult(),
      rawFormatter: (selectResult: { [key: string]: unknown }): SelectResult<VT> => {
        const raw = { result: {} as any }
        effectCbArr.forEach((effect) => effect(raw, selectResult))
        return raw.result
      }
    }
  }

  selectAll<VT extends SqlViewTemplate>(view: SqlView<VT>) {
    return this.rawSelectAll(view, {
      order: true,
    })
  }

  aggrateView<VT1 extends SqlViewTemplate, VT2 extends SqlViewTemplate>(
    view: SqlView<VT1>,
    getTemplate: (expr: (target: (ref: GetColumnHolder<VT1>) => string) => Column<boolean, unknown>) => VT2,
  ) {
    return view
      .bracketIf(({ state }) => hasOneOf(state, ['groupBy', 'skip', 'take']))
      .mapTo((e, define) => getTemplate((getTarget) => define((refStr) => getTarget((ref) => refStr(ref(e)))).withNull(true)))
      .pipe((view) => this.rawSelectAll(view, { order: false }))
  }
}