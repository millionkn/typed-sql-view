import { BuildCtx, Column, SqlState, exec, sym } from "./define.js"

export class SelectBody {
  constructor(
    private initCtx: BuildCtx,
    public opts: {
      from: {
        alias: string,
        segment: string,
      },
      join: {
        type: 'left' | 'inner',
        alias: string,
        segment: string,
        condation: string,
      }[],
      where: string[],
      groupBy: string[],
      having: string[],
      order: {
        order: 'asc' | 'desc',
        segment: string,
      }[],
      take: null | number,
      skip: number,
    },
  ) { }

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

  bracket(usedColumn: Column<boolean, unknown, string>[]) {
    const tableAlias = this.initCtx.getAlias()
    const usedInner = [...new Set(usedColumn.map((c)=>c[sym](false).inner))]  
    return new SelectBody(this.initCtx, {
      from: {
        alias: tableAlias,
        segment: exec(() => {
          const usedInnerInfo = usedInner.map((inner, index) => {
            const preExpr = inner.expr
            const columnAlias = `value_${index}`
            inner.expr = `${tableAlias}.${columnAlias}`
            return { inner, preExpr, alias: columnAlias }
          })
          const cbArr = usedInnerInfo.map(({ inner, preExpr: temp }) => {
            const current = inner.expr
            inner.expr = temp
            return () => inner.expr = current
          })
          const result = `(${this.buildSql(usedInnerInfo.map((e) => {
            return {
              select:e.
            }
          }))})`
          cbArr.forEach((cb) => cb())
          return result
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

  buildSql(selectArr: { select: string, alias: string }[]) {
    let buildResult: string[] = []
    exec(() => {
      const e = this.opts.from
      buildResult.push(`from ${e.segment} ${e.alias}`)
    })
    this.opts.join.forEach((join) => {
      buildResult.push(`${join.type} join ${join.segment} ${join.alias} ${join.condation.length === 0 ? '' : `on ${join.condation}`}`)
    })
    exec(() => {
      if (this.opts.where.length === 0) { return }
      buildResult.push(`where ${this.opts.where.join(' and ')}`)
    })
    exec(() => {
      if (this.opts.groupBy.length === 0) { return }
      buildResult.push(`group by ${this.opts.groupBy.join(',')}`)
    })
    exec(() => {
      if (this.opts.having.length === 0) { return }
      buildResult.push(`having ${this.opts.having.join(' and ')}`)
    })
    exec(() => {
      if (this.opts.order.length === 0) { return }
      buildResult.push(`order by ${this.opts.order.map((e) => `${e.segment} ${e.order}`).join(',')}`)
    })
    if (this.opts.skip) {
      buildResult.push(this.initCtx.skip(this.opts.skip))
    }
    if (this.opts.take !== null) {
      buildResult.push(this.initCtx.take(this.opts.take))
    }
    return `select ${selectArr.map(({ select, alias }) => `${select} ${alias}`).join(',') ?? '1'} ${buildResult.join(' ')}`
  }
}