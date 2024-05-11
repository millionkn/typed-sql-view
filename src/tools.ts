

export class InnerColumn {
  constructor(
    public resolvable: Resolvable
  ) { }
}

const columnSym = Symbol()

export class Column<N extends boolean = boolean, R = unknown, T extends string | null = string | null> {
  public [columnSym] = {
    withNull: true,
    format: (raw: unknown): R => { throw new Error() },
    tag: null as T,
  }
  private constructor(private inner: InnerColumn) { }
  static create(inner: InnerColumn) {
    return new Column<boolean, unknown, null>(inner)
  }


  readonly withNull = <const N extends boolean>(value: N): Column<N, R, T> => {
    const r = new Column<N, R, T>(this.inner)
    r[columnSym] = {
      ...this[columnSym],
      withNull: value
    }
    return r
  }

  readonly format = <R2>(value: (raw: unknown, format: (raw: unknown) => R) => R2): Column<N, R2, T> => {
    const r = new Column<N, R2, T>(this.inner)
    r[columnSym] = {
      ...this[columnSym],
      format: (raw) => value(raw, (raw) => this[columnSym].format(raw))
    }
    return r
  }

  readonly assert = <T1 extends T, T2 extends string | null = T1>(preTag: T1, curTag?: T2): Column<N, R, T2> => {
    if (this[columnSym].tag !== preTag) {
      throw new Error(`assert tag ${this[columnSym].tag === null ? `{null}` : `'${this[columnSym].tag}'`},but saved is ${this[columnSym].tag === null ? `{null}` : `'${this[columnSym].tag}'`}`)
    }
    if (curTag === undefined || preTag as any === curTag) { return this as any }
    const r = new Column<N, R, T2>(this.inner)
    r[columnSym] = {
      ...this[columnSym],
      tag: curTag,
    }
    return r
  }

  static getOpts<N extends boolean = boolean, R = unknown, T extends string | null = string | null>(column: Column<N, R, T>) {
    return {
      inner: column.inner,
      ...column[columnSym],
    }
  }
}

export type DeepTemplate<I> = I | (readonly DeepTemplate<I>[]) | { readonly [key: string]: DeepTemplate<I> }
export type SqlViewTemplate = DeepTemplate<Column>

export type ColumnDeclareFun<I> = (columnExpr: (input: I) => string) => Column<boolean, unknown, null>

export type GetRefStr<VT extends SqlViewTemplate> = (ref: (template: VT) => Column) => string

type _Relation<N extends boolean, VT extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }> = {
  [key in keyof VT]
  : VT[key] extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate } ? _Relation<N, VT[key]>
  : VT[key] extends Column<infer N2, infer X, infer T> ? Column<(N2 & N) extends true ? true : boolean, X, T>
  : never
}

export type Relation<N extends boolean, VT extends SqlViewTemplate> = N extends false ? VT
  : VT extends Column<infer N2, infer X, infer T> ? Column<(N2 & N) extends true ? true : boolean, X, T>
  : _Relation<N, Extract<VT, readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }>>

export type Segment<T = unknown> = Array<string | { value: T }>
export type SqlState = 'leftJoin' | 'innerJoin' | 'where' | 'groupBy' | 'having' | 'take' | 'skip' | 'order'
export type Resolvable = (ctx: SqlContext) => string
export type SqlContext = {
  resolveSym: (sym: object) => string,
  genTableAlias: () => string,
  setParam: (value: any) => string,
  sym: {
    skip: string,
    take: string,
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

export function getSegmentTarget<T, R>(segment: Segment<T>, filterMap: (v: T) => false | R): R[] {
  return segment.map((e) => typeof e === 'object' && filterMap(e.value)).filter((r): r is R => !!r)
}

export function segmentToStr<T>(segment: Segment<T>, toStr: (value: T) => string) {
  return segment.map((e) => typeof e === 'string' ? e : toStr(e.value)).join('')
}
