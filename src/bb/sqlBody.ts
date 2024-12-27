import { Adapter, exec } from "./define.js"

export type SqlBody = {
  from: {
    alias: string,
    segment: string,
  }[],
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
}

export type SqlState = 'leftJoin' | 'innerJoin' | 'where' | 'groupBy' | 'having' | 'take' | 'skip' | 'order'

export function sqlBodyState(body: SqlBody) {
  const state = new Set<SqlState>()
  if (body.join.some((e) => e.type === 'inner')) { state.add('innerJoin') }
  if (body.join.some((e) => e.type === 'left')) { state.add('leftJoin') }
  if (body.where && body.where.length > 0) { state.add('where') }
  if (body.groupBy && body.groupBy.length > 0) { state.add('groupBy') }
  if (body.having && body.having.length > 0) { state.add('having') }
  if (body.order && body.order.length > 0) { state.add('order') }
  if ((body.skip ?? 0) > 0) { state.add('skip') }
  if (body.take !== null) { state.add('take') }
  return state
}

export function buildSqlBodyStr(
  adapter: Adapter,
  opts: SqlBody,
) {
  let buildResult: string[] = []
  if (opts.from.length === 0) { return '' }
  buildResult.push(`from ${opts.from.map((e) => `${e.segment} ${e.alias}`).join(',')}`)
  opts.join.forEach((join) => {
    buildResult.push(`${join.type} join ${join.segment} ${join.alias} ${join.condation.length === 0 ? '' : `on ${join.condation}`}`)
  })
  exec(() => {
    if (opts.where.length === 0) { return }
    buildResult.push(`where ${opts.where.join(' and ')}`)
  })
  exec(() => {
    if (opts.groupBy.length === 0) { return }
    buildResult.push(`group by ${opts.groupBy.join(',')}`)
  })
  exec(() => {
    if (opts.having.length === 0) { return }
    buildResult.push(`having ${opts.having.join(' and ')}`)
  })
  exec(() => {
    if (opts.order.length === 0) { return }
    buildResult.push(`order by ${opts.order.map((e) => `${e.segment} ${e.order}`).join(',')}`)
  })
  if (opts.skip) {
    buildResult.push(adapter.skip(opts.skip))
  }
  if (opts.take !== null) {
    buildResult.push(adapter.take(opts.take))
  }
  return buildResult.join(' ')
}
