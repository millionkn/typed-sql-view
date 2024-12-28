export const sym = Symbol()

export const exec = <T>(fun: () => T): T => fun()

export type DeepTemplate<I> = I | (readonly [...DeepTemplate<I>[]]) | { readonly [key: string]: DeepTemplate<I> }

export function flatViewTemplate<T extends string>(template: SqlViewTemplate<T>): Column<T, unknown, boolean>[] {
  if (template instanceof Column) { return [template] }
  if (template instanceof Array) { return template.flatMap((e) => flatViewTemplate(e)) }
  return Object.values(template).flatMap((e) => flatViewTemplate(e))
}

export type Inner = {
  declareUsed: () => void,
  expr: string,
}

export function createColumn(expr: string) { 
  
}

export class Column<T extends string, R = unknown, N extends boolean = boolean> {
  static [sym](inner: Inner) {
    return new Column({
      inner,
      assert: '',
      format: (raw) => raw,
      withNull: true,
    })
  }

  private constructor(
    opts: Column<T, R, N>[typeof sym]
  ) { this[sym] = opts }

  [sym]: {
    inner: Inner,
    withNull: N,
    format: (raw: unknown) => R,
    assert: T,
  }


  withNull<const N extends boolean>(value: N) {
    return new Column<T, R, N>({
      ...this[sym],
      withNull: value,
    })
  }

  format<R2>(value: (raw: unknown, format: (raw: unknown) => R) => R2) {
    const format = this[sym].format
    return new Column<T, R2, N>({
      ...this[sym],
      format: (raw) => value(raw, format)
    })
  }

  assert<T2 extends string>(pre: T, cur: T2) {
    if (this[sym].assert !== pre) {
      throw new Error(`assert tag '${pre}',but saved is '${this[sym].assert}'`)
    }
    return new Column<T2, R, N>({
      ...this[sym],
      assert: cur,
    })
  }

  declareUsed() {
    return this[sym].inner.declareUsed()
  }
}

export type SqlViewTemplate<T extends string> = DeepTemplate<Column<T>>

type _Relation<N extends boolean, VT extends readonly SqlViewTemplate<string>[] | { readonly [key: string]: SqlViewTemplate<string> }> = {
  [key in keyof VT]
  : VT[key] extends readonly SqlViewTemplate<string>[] | { readonly [key: string]: SqlViewTemplate<string> } ? _Relation<N, VT[key]>
  : VT[key] extends Column<infer T, infer R, infer N2> ? Column<T, R, (N2 & N) extends true ? true : boolean>
  : never
}

export type Relation<N extends boolean, VT extends SqlViewTemplate<string>> = N extends false ? VT
  : VT extends Column<infer T, infer R, infer N2> ? Column<T, R, (N2 & N) extends true ? true : boolean>
  : VT extends readonly SqlViewTemplate<string>[] ? _Relation<N, VT>
  : VT extends { readonly [key: string]: SqlViewTemplate<string> } ? _Relation<N, VT>
  : never

export type SelectResult<VT extends SqlViewTemplate<''>> = VT extends readonly [] ? []
  : VT extends readonly [infer X extends SqlViewTemplate<''>, ...infer Arr extends readonly SqlViewTemplate<''>[]]
  ? [SelectResult<X>, ...SelectResult<Arr>]
  : VT extends readonly (infer X extends SqlViewTemplate<''>)[]
  ? SelectResult<X>[]
  : VT extends Column<infer X, infer Y>
  ? (true extends X ? null : never) | Y
  : VT extends { [key: string]: SqlViewTemplate<''> }
  ? { -readonly [key in keyof VT]: SelectResult<VT[key]> }
  : never

export type SqlAdapter = {
  skip: (value: number) => string,
  take: (value: number) => string,
  paramHolder: (index: number) => string,
}
export type Adapter = {
  skip: (value: number) => string,
  take: (value: number) => string,
}

export type CreateResolver = <V>() => {
  createHolder: (getValue: () => V) => string;
  resolve: (str: string) => (string | (() => V))[];
}
export type BuildCtx = {
  adapter: Adapter,
  genAlias: () => string,
  setParam: (value: unknown) => string,
  createResolver: CreateResolver,
}

export type GetColumnRef<T> = (ref: (e: T) => Column<''>) => string


export function hasOneOf<T>(items: Iterable<T>, arr: NoInfer<T>[]) {
  return !![...items].find((e) => arr.includes(e))
}

export type SqlBody = {
  from: {
    alias: string,
    expr: string,
  }[],
  join: {
    type: 'left' | 'inner',
    alias: string,
    expr: string,
    condation: string,
  }[],
  where: Array<string>,
  groupBy: Array<string>,
  having: Array<string>,
  order: {
    order: 'asc' | 'desc',
    expr: string,
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


export function pickConfig<K extends string, R>(key: K, config: { [key in K]: () => R }): R {
  return config[key]()
}

export function buildSqlBodyStr(adapter: Adapter, sqlBody: SqlBody) {
  if (sqlBody.from.length === 0) { return '' }
  const buildResult: string[] = []
  buildResult.push(`${sqlBody.from.map((e) => `${e.expr} ${e.alias}`).join(',')}`)
  sqlBody.join.forEach((join) => {
    const condation = join.condation
    buildResult.push(`${join.type} join ${join.expr} ${join.alias} ${condation.length === 0 ? '' : `on ${condation}`}`)
  })
  exec(() => {
    if (sqlBody.where.length === 0) { return }
    buildResult.push(`where ${sqlBody.where.join(' and ')}`)
  })
  exec(() => {
    if (sqlBody.groupBy.length === 0) { return }
    buildResult.push(`group by ${sqlBody.groupBy.join(',')}`)
  })
  exec(() => {
    if (sqlBody.having.length === 0) { return }
    buildResult.push(`having ${sqlBody.having.join(' and ')}`)
  })
  exec(() => {
    if (sqlBody.order.length === 0) { return }
    buildResult.push(`order by ${sqlBody.order.map((e) => `${e.expr} ${e.order}`).join(',')}`)
  })
  if (sqlBody.skip) {
    buildResult.push(adapter.skip(sqlBody.skip))
  }
  if (sqlBody.take !== null) {
    buildResult.push(adapter.take(sqlBody.take))
  }
  return buildResult.join(' ').trim()
}
