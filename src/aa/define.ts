export const sym = Symbol()

export const exec = <T>(fun: () => T): T => fun()

export type DeepTemplate<I> = I | (readonly DeepTemplate<I>[]) | { readonly [key: string]: DeepTemplate<I> }

export type Inner = {
  expr: string
}

export function hasOneOf<T>(items: Iterable<T>, arr: NoInfer<T>[]) {
  return !![...items].find((e) => arr.includes(e))
}

export const resolveSqlStr = exec(() => {
  let _nsIndex = 0
  return <V>(getExpr: (holder: (value: V) => string) => string) => {
    const nsIndex = _nsIndex += 1
    const split = `'"'"split_${nsIndex}'"'"`
    let index = 0
    const saved = new Map<string, V>()
    const expr = getExpr((value) => {
      const key = `holder_${index += 1}`
      saved.set(key, value)
      return `${split}${key}${split}`
    })
    _nsIndex -= 1
    return expr.length === 0 ? [] : expr.split(split).map((str, i) => {
      if (i % 2 === 0) { return str }
      const r = saved.get(str)
      if (!r) { throw new Error() }
      return r
    })
  }
})

function _flatViewTemplate(template: SqlViewTemplate): Column<boolean, unknown, string>[] {
  if (template instanceof Column) { return [template] }
  if (template instanceof Array) { return template.flatMap((e) => _flatViewTemplate(e)) }
  return Object.values(template).flatMap((e) => _flatViewTemplate(e))
}

export function flatViewTemplate(template: SqlViewTemplate) {
  return _flatViewTemplate(template)
}

export class Column<N extends boolean = boolean, R = unknown, T extends string = ''> {
  static [sym](expr: string) {
    return new Column({
      inner: { expr },
      assert: '',
      format: (raw) => raw,
      withNull: true,
    })
  }

  private constructor(
    private opts: {
      inner: Inner,
      withNull: N,
      format: (raw: unknown) => R,
      assert: T,
    }
  ) { }

  [sym](strict: boolean) {
    if (strict && this.opts.assert !== '') {
      throw new Error(`column should assert '${this.opts.assert}'`)
    }
    return this.opts
  }


  withNull<const N extends boolean>(value: N) {
    return new Column<N, R, T>({
      ...this.opts,
      withNull: value,
    })
  }

  format<R2>(value: (raw: unknown, format: (raw: unknown) => R) => R2): Column<N, R2, T> {
    const format = this.opts.format
    return new Column<N, R2, T>({
      ...this.opts,
      format: (raw) => value(raw, format)
    })
  }

  assert<T2 extends string>(pre: T, cur: T2): Column<N, R, T2> {
    if (this.opts.assert !== pre) {
      throw new Error(`assert tag '${pre}',but saved is '${this.opts.assert}'`)
    }
    return new Column<N, R, T2>({
      ...this.opts,
      assert: cur,
    })
  }
}

export type ColumnDeclareFun<T> = (columnExpr: (input: T) => string) => Column

export type SqlViewTemplate = DeepTemplate<Column<boolean, unknown, string>>

type _Relation<N extends boolean, VT extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }> = {
  [key in keyof VT]
  : VT[key] extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate } ? _Relation<N, VT[key]>
  : VT[key] extends Column<infer N2, infer X, infer T> ? Column<(N2 & N) extends true ? true : boolean, X, T>
  : never
}

export type Relation<N extends boolean, VT extends SqlViewTemplate> = N extends false ? VT
  : VT extends Column<infer N2, infer X, infer T> ? Column<(N2 & N) extends true ? true : boolean, X, T>
  : _Relation<N, Extract<VT, readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }>>


export type SqlViewSelectTemplate = DeepTemplate<Column>
export type SelectResult<VT extends SqlViewSelectTemplate> = VT extends readonly [] ? []
  : VT extends readonly [infer X extends SqlViewSelectTemplate, ...infer Arr extends readonly SqlViewSelectTemplate[]]
  ? [SelectResult<X>, ...SelectResult<Arr>]
  : VT extends Column<infer X, infer Y>
  ? (true extends X ? null : never) | Y
  : VT extends { [key: string]: SqlViewSelectTemplate }
  ? { -readonly [key in keyof VT]: SelectResult<VT[key]> }
  : never


export type BuildCtx = {
  getAlias: () => string,
  setParam: (value: unknown) => string,
  skip: (value: number) => string,
  take: (value: number) => string,
}

export type GetColumnHolder<T> = (ref: (e: T) => Column) => string

export type SqlState = 'leftJoin' | 'innerJoin' | 'where' | 'groupBy' | 'having' | 'take' | 'skip' | 'order'
