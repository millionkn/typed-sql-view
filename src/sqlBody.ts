import { exec } from "./private.js"
import { AliasSym, Segment, SqlState, Inner, BuildContext, InitContext, AsyncStr } from './define.js'
import { resolveSqlStr } from "./tools.js"

class SegmentResolver {
  register = (aliasSym: AliasSym, alias: string) => {
    aliasSym.getAlias = () => alias
  }
  resolveSym = (aliasSym: AliasSym) => {
    return aliasSym.getAlias()
  }
  resolveSegment = (segment: Segment) => {
    return segment.map((e) => typeof e === 'string' ? e : this.resolveSym(e)).join('')
  }
}

export class SqlBody {
  constructor(
    private initCtx: InitContext,
    public opts: {
      from: {
        aliasSym: AliasSym,
        getStr: AsyncStr,
      },
      join: {
        type: 'left' | 'inner',
        aliasSym: AliasSym,
        getStr: AsyncStr,
        condation: Segment,
      }[],
      where: AsyncStr[],
      groupBy: AsyncStr[],
      having: AsyncStr[],
      order: {
        order: 'asc' | 'desc',
        segment: AsyncStr,
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
        getStr: exec(() => {
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
    const resolver = new SegmentResolver()
    let bodyArr: string[] = []
    exec(() => {
      if (!this.opts.from) { return }
      const tableAlias = this.initCtx.genTableAlias()
      bodyArr.push(`${resolver.resolveSegment(this.opts.from.segment)} as "${tableAlias}"`)
      resolver.register(this.opts.from.aliasSym, tableAlias)
    })
    this.opts.join.forEach((join) => {
      const tableAlias = this.initCtx.genTableAlias()
      const body = resolver.resolveSegment(join.segment)
      const str1 = `${join.type} join ${body} as "${tableAlias}"`
      resolver.register(join.aliasSym, tableAlias)
      bodyArr.push(`${str1} on ${resolver.resolveSegment(join.condation)}`)
    })
    exec(() => {
      if (this.opts.where.length === 0) { return }
      bodyArr.push(`where`)
      bodyArr.push(this.opts.where.map(resolver.resolveSegment).filter((v) => v.length !== 0).join(' and '))
    })
    exec(() => {
      if (this.opts.groupBy.length === 0) { return }
      bodyArr.push(`group by`)
      bodyArr.push(this.opts.groupBy.map(resolver.resolveSegment).join(','))
    })
    exec(() => {
      if (this.opts.having.length === 0) { return }
      bodyArr.push(`having`)
      bodyArr.push(this.opts.having.map(resolver.resolveSegment).filter((v) => v.length !== 0).join(' and '))
    })
    exec(() => {
      if (this.opts.order.length === 0) { return }
      bodyArr.push(`order by`)
      bodyArr.push(this.opts.order.map(({ segment, order }) => `${resolver.resolveSegment(segment)} ${order}`).join(','))
    })
    if (this.opts.skip) {
      bodyArr.push(`${this.initCtx.language.skip} ${this.opts.skip}`)
    }
    if (this.opts.take !== null) {
      bodyArr.push(`${this.initCtx.language.take} ${this.opts.take}`)
    }
    const fromBody = bodyArr.join(' ').trim()
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