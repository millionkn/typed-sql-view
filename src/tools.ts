export class Column<N extends boolean = boolean, R = unknown> {
  private resolvable: Resolvable = () => { throw new Error() }
  constructor(
    opts: {
      withNull: N,
      format: (raw: unknown) => R,
    }
  ) { }

  static setResolvable(column: Column, resolvable: Resolvable) {
    column.resolvable = resolvable
  }
  static getResolvable(column: Column) {
    if (!column.resolvable) { throw new Error() }
    return column.resolvable
  }
}

export type DeepTemplate<I> = I | (readonly DeepTemplate<I>[]) | { readonly [key: string]: DeepTemplate<I> }
export type SqlViewTemplate = DeepTemplate<Column>

export type ColumnDeclareFun<I> = <N extends boolean, R = unknown>(withNull: N, columnExpr: (input: I) => string, formatter?: (notNull: unknown) => R) => Column<N extends false ? false : boolean, R>

export type GetRefStr<VT extends SqlViewTemplate> = (ref: (template: VT) => Column) => string

type _Relation<VT extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }> = {
  [key in keyof VT]
  : VT[key] extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate } ? _Relation<VT[key]>
  : VT[key] extends Column<boolean, infer X> ? Column<boolean, X>
  : never
}

export type Relation<N extends boolean, VT extends SqlViewTemplate> = N extends false ? VT
  : VT extends Column<boolean, infer X> ? Column<true, X>
  : _Relation<Extract<VT, readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }>>

export type Segment<T = unknown> = Array<string | { value: T }>
export type SqlState = 'leftJoin' | 'innerJoin' | 'where' | 'groupBy' | 'having' | 'take' | 'skip' | 'order'
export type Resolvable = (ctx: SqlContext) => string
export type SqlContext = {
  resolveSym: (sym: object) => string,
  genTableAlias: () => string,
  setParam: (value: any) => string,
  sym: {
    skip: 'skip' | 'offset',
    take: 'take' | 'limit',
  }
}
export const exec = <T>(fun: () => T): T => fun()

export const resolveExpr = exec(() => {
  let _nsIndex = 0
  return <T>(getExpr: (holder: (value: T) => string) => string): Segment<T> => {
    const nsIndex = _nsIndex += 1
    let index = 0
    const saved = new Map<string, { value: T }>()
    const expr = getExpr((value) => {
      const key = `holder_${nsIndex}_${index += 1}`
      saved.set(key, { value })
      return `'"'"${key}'"'"`
    })
    const resultArr = expr.split(`'"'"`).map((str, i) => {
      if (i % 2 === 0) { return str }
      const r = saved.get(str)
      if (!r) { return `'"'"${str}'"'"` }
      return r
    })
    _nsIndex -= 1
    return resultArr
  }
})

export function hasOneOf<T>(items: T[], arr: (T & {})[]) {
  return !!items.find((e) => arr.includes(e as any))
}

export function pickConfig<K extends string | number, R>(key: K, config: { [key in K]: () => R }): R {
  return config[key]()
}