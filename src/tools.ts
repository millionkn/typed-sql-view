export class Column<N extends boolean = boolean, R = unknown> {
  private share = { resolvable: (() => { throw new Error() }) as Resolvable }
  constructor(
    private opts: {
      withNull: N,
      format: (raw: unknown) => R
    }
  ) { }

  readonly withNull = <N extends boolean>(value: N): Column<N extends false ? false : boolean, R> => {
    const r = new Column({ withNull: value as any, format: this.opts.format })
    r.share = this.share
    return r
  }

  readonly format = <R>(value: (raw: unknown) => R): Column<N, R> => {
    const r = new Column({ withNull: this.opts.withNull, format: value as () => any })
    r.share = this.share
    return r
  }

  static getOpts(column: Column) {
    return column.opts
  }

  static setResolvable(column: Column, resolvable: Resolvable) {
    column.share.resolvable = resolvable
  }
  static getResolvable(column: Column) {
    return column.share.resolvable
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
    const split = `'"${nsIndex}'"`
    let index = 0
    const saved = new Map<string, { value: T }>()
    const expr = getExpr((value) => {
      const key = `holder_${nsIndex}_${index += 1}`
      saved.set(key, { value })
      return `${split}${key}${split}`
    })
    const resultArr = expr.length === 0 ? [] : expr.split(split).map((str, i) => {
      if (i % 2 === 0) { return str }
      const r = saved.get(str)
      if (!r) { throw new Error() }
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

function _flatViewTemplate(template: SqlViewTemplate): Column[] {
  if (template instanceof Column) { return [template] }
  if (template instanceof Array) { return template.flatMap((e) => _flatViewTemplate(e)) }
  return Object.values(template).flatMap((e) => _flatViewTemplate(e))
}

export function flatViewTemplate(template: SqlViewTemplate): Column[] {
  return [...new Set(_flatViewTemplate(template))]
}

export function getSegmentTarget<T>(segment: Segment<T>): T[] {
  return segment.map((e) => typeof e === 'object' && e.value).filter((e): e is T => !!e)
}

export function segmentToStr<T>(segment: Segment<T>, toStr: (value: T) => string) {
  return segment.map((e) => typeof e === 'string' ? e : toStr(e.value)).join('')
}
