

export class InnerColumn {
  constructor(
    public resolvable: Resolvable
  ) { }
}

export class Column<N extends boolean = boolean, R = unknown> {
  private _withNull = true
  private _format = (raw: unknown): R => { throw new Error() }
  constructor(private inner: InnerColumn) { }

  readonly withNull = <const N extends boolean>(value: N): Column<N, R> => {
    const r = new Column<N, R>(this.inner)
    r._withNull = value
    r._format = r._format
    return r
  }

  readonly format = <R>(value: (raw: unknown) => R): Column<N, R> => {
    const r = new Column<N, R>(this.inner)
    r._withNull = r._withNull
    r._format = value
    return r
  }

  static getOpts(column: Column) {
    return {
      inner: column.inner,
      format: column._format,
      withNull: column._withNull,
    }
  }
}

export type DeepTemplate<I> = I | (readonly DeepTemplate<I>[]) | { readonly [key: string]: DeepTemplate<I> }
export type SqlViewTemplate = DeepTemplate<Column>

export type ColumnDeclareFun<I> = (columnExpr: (input: I) => string) => Column<boolean, unknown>

export type GetRefStr<VT extends SqlViewTemplate> = (ref: (template: VT) => Column) => string

type _Relation<N extends boolean, VT extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }> = {
  [key in keyof VT]
  : VT[key] extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate } ? _Relation<N, VT[key]>
  : VT[key] extends Column<infer N2, infer X> ? Column<(N2 & N) extends true ? true : boolean, X>
  : never
}

export type Relation<N extends boolean, VT extends SqlViewTemplate> = N extends false ? VT
  : VT extends Column<infer N2, infer X> ? Column<(N2 & N) extends true ? true : boolean, X>
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

export function getSegmentTarget<T>(segment: Segment<T>): T[] {
  return segment.map((e) => typeof e === 'object' && e.value).filter((e): e is T => !!e)
}

export function segmentToStr<T>(segment: Segment<T>, toStr: (value: T) => string) {
  return segment.map((e) => typeof e === 'string' ? e : toStr(e.value)).join('')
}
