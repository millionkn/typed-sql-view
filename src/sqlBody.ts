import { Resolvable, SqlState, SqlContext, exec } from "./tools.js"

export class SqlBody {
  constructor(public opts: {
    from: {
      aliasSym: object,
      resolvable: Resolvable,
    },
    join: {
      type: 'left' | 'inner',
      aliasSym: object,
      resolvable: Resolvable,
      condation: Resolvable,
    }[],
    where: Resolvable[],
    groupBy: Resolvable[],
    having: Resolvable[],
    order: {
      order: 'asc' | 'desc',
      resolvable: Resolvable,
    }[],
    take: null | number,
    skip: number,
  }) { }

  state() {
    const base = this.opts
    const stateArr: SqlState[] = []
    if (base.join.some((e) => e.type === 'inner')) { stateArr.push('innerJoin') }
    if (base.join.some((e) => e.type === 'left')) { stateArr.push('leftJoin') }
    if (base.where && base.where.length > 0) { stateArr.push('where') }
    if (base.groupBy && base.groupBy.length > 0) { stateArr.push('groupBy') }
    if (base.having && base.having.length > 0) { stateArr.push('having') }
    if (base.order && base.order.length > 0) { stateArr.push('order') }
    if ((base.skip ?? 0) > 0) { stateArr.push('skip') }
    if (base.take !== null) { stateArr.push('take') }
    return stateArr
  }

  build(select: Map<Resolvable, string>, ctx: SqlContext) {
    const ctxAliasMapper = new Map<object, string>()
    let bodyArr: string[] = []
    const resolveSym = (sym: object) => ctxAliasMapper.get(sym) ?? ctx.resolveSym(sym)

    exec(() => {
      if (!this.opts.from) { return }
      const tableAlias = ctx.genTableAlias()
      const exprStr = this.opts.from.resolvable(ctx)
      bodyArr.push(`${exprStr} as "${tableAlias}"`)
      ctxAliasMapper.set(this.opts.from.aliasSym, tableAlias)
    })
    this.opts.join.forEach((join) => {
      const tableAlias = ctx.genTableAlias()
      const body = join.resolvable(ctx)
      const str1 = `${join.type} join ${body} as "${tableAlias}"`
      ctxAliasMapper.set(join.aliasSym, tableAlias)
      const str2 = `on ${join.condation({ ...ctx, resolveSym })}`
      bodyArr.push(`${str1} ${str2}`)
    })
    exec(() => {
      if (this.opts.where.length === 0) { return }
      bodyArr.push(`where`)
      bodyArr.push(this.opts.where.map((segment) => segment({ ...ctx, resolveSym })).join(' and '))
    })
    exec(() => {
      if (this.opts.groupBy.length === 0) { return }
      bodyArr.push(`group by`)
      bodyArr.push(this.opts.groupBy.map((segment) => segment({ ...ctx, resolveSym })).join(','))
    })
    exec(() => {
      if (this.opts.having.length === 0) { return }
      bodyArr.push(`having`)
      bodyArr.push(this.opts.having.map((segment) => segment({ ...ctx, resolveSym })).join(' and '))
    })
    exec(() => {
      if (this.opts.order.length === 0) { return }
      bodyArr.push(`order by`)
      bodyArr.push(this.opts.order.map(({ resolvable: segment, order }) => `${segment({ ...ctx, resolveSym })} ${order}`).join(','))
    })
    if (this.opts.skip) {
      bodyArr.push(`${ctx.sym.skip} ${this.opts.skip}`)
    }
    if (this.opts.take !== null) {
      bodyArr.push(`${ctx.sym.take} ${this.opts.take}`)
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