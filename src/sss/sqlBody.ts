import { ExprResolver, SqlState, BuildContext, exec, Column, privateSym, SegmentResolver } from "./tools.js"

export class SqlBody {
  constructor(public opts: {
    from: {
      aliasSym: object,
      segment: SegmentResolver,
    },
    join: {
      type: 'left' | 'inner',
      aliasSym: object,
      segment: SegmentResolver,
      condation: SegmentResolver,
    }[],
    where: SegmentResolver[],
    groupBy: SegmentResolver[],
    having: SegmentResolver[],
    order: {
      order: 'asc' | 'desc',
      segment: SegmentResolver,
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

  bracket(usedColumn: Column[]) {
    const usedInnerInfo = [...new Set(usedColumn.map((e) => Column[privateSym].getOpts(e).inner))].map((inner, index) => {
      const temp = inner.exprResolver
      const alias = `value_${index}`
      inner.exprResolver = (ctx) => `"${ctx.resolveSym(aliasSym)}"."${alias}"`
      return { inner, temp, alias }
    })
    const aliasSym = {}
    return new SqlBody({
      from: {
        aliasSym,
        segment: (ctx) => {
          const cbArr = usedInnerInfo.map(({ inner, temp }) => {
            const current = inner.exprResolver
            inner.exprResolver = temp
            return () => inner.exprResolver = current
          })
          const result = `(${this.build(new Map(usedInnerInfo.map((e) => [e.inner.exprResolver, e.alias])), ctx)})`
          cbArr.forEach((cb) => cb())
          return result
        },
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

  build(select: Map<ExprResolver, string>, ctx: BuildContext) {
    const ctxAliasMapper = new Map<object, string>()
    let bodyArr: string[] = []
    const resolveSym = (sym: object) => ctxAliasMapper.get(sym) ?? ctx.resolveSym(sym)

    exec(() => {
      if (!this.opts.from) { return }
      const tableAlias = ctx.genTableAlias()
      bodyArr.push(`${this.opts.from.segment(ctx)} as "${tableAlias}"`)
      ctxAliasMapper.set(this.opts.from.aliasSym, tableAlias)
    })
    this.opts.join.forEach((join) => {
      const tableAlias = ctx.genTableAlias()
      const body = join.segment(ctx)
      const str1 = `${join.type} join ${body} as "${tableAlias}"`
      ctxAliasMapper.set(join.aliasSym, tableAlias)
      const str2 = `on ${join.condation({ ...ctx, resolveSym })}`
      bodyArr.push(`${str1} ${str2}`)
    })
    exec(() => {
      if (this.opts.where.length === 0) { return }
      bodyArr.push(`where`)
      bodyArr.push(this.opts.where.map((resolvable) => resolvable({ ...ctx, resolveSym })).filter((v) => v.length !== 0).join(' and '))
    })
    exec(() => {
      if (this.opts.groupBy.length === 0) { return }
      bodyArr.push(`group by`)
      bodyArr.push(this.opts.groupBy.map((resolvable) => resolvable({ ...ctx, resolveSym })).join(','))
    })
    exec(() => {
      if (this.opts.having.length === 0) { return }
      bodyArr.push(`having`)
      bodyArr.push(this.opts.having.map((resolvable) => resolvable({ ...ctx, resolveSym })).filter((v) => v.length !== 0).join(' and '))
    })
    exec(() => {
      if (this.opts.order.length === 0) { return }
      bodyArr.push(`order by`)
      bodyArr.push(this.opts.order.map(({ segment: resolvable, order }) => `${resolvable({ ...ctx, resolveSym })} ${order}`).join(','))
    })
    if (this.opts.skip) {
      bodyArr.push(`${ctx.language.skip} ${this.opts.skip}`)
    }
    if (this.opts.take !== null) {
      bodyArr.push(`${ctx.language.take} ${this.opts.take}`)
    }
    const fromBody = bodyArr.join(' ')
    const selectTarget = select.size === 0 ? '1' : [...select.entries()]
      .map(([resolvable, alias]) => `${resolvable({ ...ctx, resolveSym })} as "${alias}"`)
      .join(',')
    if (fromBody.length === 0) {
      return `select ${selectTarget}`
    } else {
      return `select ${selectTarget} from ${fromBody}`
    }
  }
}