import { exec } from "./private.js"
import { AliasSym, Segment, SqlState, Inner, InitContext, AsyncStr } from './define.js'

export class SqlBody {
  constructor(
    private initCtx: InitContext,
    public opts: {
      from: {
        aliasSym: AliasSym,
        segment: Segment,
      },
      join: {
        type: 'left' | 'inner',
        aliasSym: AliasSym,
        segment: Segment,
        getCondation: AsyncStr,
      }[],
      where: AsyncStr[],
      groupBy: AsyncStr[],
      having: AsyncStr[],
      order: {
        order: 'asc' | 'desc',
        getStr: AsyncStr,
      }[],
      take: null | number,
      skip: number,
    }) { }

  public state() {
    const base = this.opts
    const state = new Set<SqlState>()
    if (base.join.some((e) => e.type === 'inner')) { state.add('innerJoin') }
    if (base.join.some((e) => e.type === 'left')) { state.add('leftJoin') }
    if (base.where && base.where.length > 0) { state.add('where') }
    if (base.groupBy && base.groupBy.length > 0) { state.add('groupBy') }
    if (base.having && base.having.length > 0) { state.add('having') }
    if (base.order && base.order.length > 0) { state.add('order') }
    if ((base.skip ?? 0) > 0) { state.add('skip') }
    if (base.take !== null) { state.add('take') }
    return state
  }

  bracket(usedInner: Inner[]) {
    const aliasSym = new AliasSym()
    return new SqlBody(this.initCtx, {
      from: {
        aliasSym,
        segment: exec(() => {
          const usedInnerInfo = [...new Set(usedInner)].map((inner, index) => {
            const temp = inner.getStr
            const alias = `value_${index}`
            inner.getStr = () => `"${aliasSym.getAlias()}"."${alias}"`
            return { inner, temp, alias }
          })
          const cbArr = usedInnerInfo.map(({ inner, temp }) => {
            const current = inner.getStr
            inner.getStr = temp
            return () => inner.getStr = current
          })
          const result = `(${this.build(new Map(usedInnerInfo.map((e) => [e.inner, e.alias])))})`
          cbArr.forEach((cb) => cb())
          return [result]
        }),
      },
      join: [],
      where: [],
      groupBy: [],
      having: [],
      order: [],
      take: null,
      skip: 0,
    })
  }

  build(select: Map<Inner, string>) {
    let buildResult: string[] = []
    exec(() => {
      if (!this.opts.from) { return }
      const tableAlias = this.initCtx.genTableAlias()
      const body = this.opts.from.segment
        .map((e) => typeof e === 'string' ? e : e.getAlias())
        .join('')
      buildResult.push(`${body} as "${tableAlias}"`)
      this.opts.from.aliasSym.getAlias = () => tableAlias
    })
    this.opts.join.forEach((join) => {
      const tableAlias = this.initCtx.genTableAlias()
      const body = join.segment
        .map((e) => typeof e === 'string' ? e : e.getAlias())
        .join('')
      const str1 = `${join.type} join ${body} as "${tableAlias}"`
      join.aliasSym.getAlias = () => tableAlias
      buildResult.push(`${str1} on ${join.getCondation()}`)
    })
    exec(() => {
      if (this.opts.where.length === 0) { return }
      buildResult.push(`where`)
      buildResult.push(this.opts.where.map((getStr) => getStr()).filter((v) => v.length !== 0).join(' and '))
    })
    exec(() => {
      if (this.opts.groupBy.length === 0) { return }
      buildResult.push(`group by`)
      buildResult.push(this.opts.groupBy.map((getStr) => getStr()).join(','))
    })
    exec(() => {
      if (this.opts.having.length === 0) { return }
      buildResult.push(`having`)
      buildResult.push(this.opts.having.map((getStr) => getStr()).filter((v) => v.length !== 0).join(' and '))
    })
    exec(() => {
      if (this.opts.order.length === 0) { return }
      buildResult.push(`order by`)
      buildResult.push(this.opts.order.map(({ getStr, order }) => `${getStr()} ${order}`).join(','))
    })
    if (this.opts.skip) {
      buildResult.push(`${this.initCtx.language.skip} ${this.opts.skip}`)
    }
    if (this.opts.take !== null) {
      buildResult.push(`${this.initCtx.language.take} ${this.opts.take}`)
    }
    const fromBody = buildResult.join(' ').trim()
    const selectTarget = select.size === 0 ? '1' : [...select.entries()]
      .map(([inner, alias]) => `${inner.getStr()} as "${alias}"`)
      .join(',')
    if (fromBody.length === 0) {
      return `select ${selectTarget}`
    } else {
      return `select ${selectTarget} from ${fromBody}`
    }
  }
}