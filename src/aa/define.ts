import { DeepTemplate, Inner, sym } from "./private.js"

export class Column<N extends boolean = boolean, R = unknown, T extends string = string> {
  static [sym](expr: string) {
    const r: DefaultColumnType = new Column()
    r[sym] = {
      inner: { expr },
      assert: '',
      format: (raw) => raw,
      hasNull: true,
    }
    return r
  }

  [sym]!: {
    inner: Inner,
    hasNull: N,
    format: (raw: unknown) => R,
    assert: T,
  }
  private constructor() { }


  withNull<const N extends boolean>(value: N) {
    const r = new Column<N, R, T>()
    r[sym] = {
      ...this[sym],
      hasNull: value,
    }
    return r
  }

  format<R2>(value: (raw: unknown, format: (raw: unknown) => R) => R2): Column<N, R2, T> {
    const format = this[sym].format
    const r = new Column<N, R2, T>()
    r[sym] = {
      ...this[sym],
      format: (raw) => value(raw, format)
    }
    return r
  }

  assert<T2 extends string>(pre: T, cur: T2): Column<N, R, T2> {
    if (this[sym].assert !== pre) {
      throw new Error(`assert tag '${pre}',but saved is '${this[sym].assert}'`)
    }
    const r = new Column<N, R, T2>()
    r[sym] = {
      ...this[sym],
      assert: cur,
    }
    return r
  }
}
export type DefaultColumnType = Column<boolean, unknown, ''>

export type ColumnDeclareFun<T> = (columnExpr: (input: T) => string) => DefaultColumnType

export type SqlViewTemplate = DeepTemplate<Column>

type _Relation<N extends boolean, VT extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }> = {
  [key in keyof VT]
  : VT[key] extends readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate } ? _Relation<N, VT[key]>
  : VT[key] extends Column<infer N2, infer X, infer T> ? Column<(N2 & N) extends true ? true : boolean, X, T>
  : never
}

export type Relation<N extends boolean, VT extends SqlViewTemplate> = N extends false ? VT
  : VT extends Column<infer N2, infer X, infer T> ? Column<(N2 & N) extends true ? true : boolean, X, T>
  : _Relation<N, Extract<VT, readonly SqlViewTemplate[] | { readonly [key: string]: SqlViewTemplate }>>


export type SelectResult<VT extends SqlViewTemplate> = VT extends readonly [] ? []
  : VT extends readonly [infer X extends SqlViewTemplate, ...infer Arr extends readonly SqlViewTemplate[]]
  ? [SelectResult<X>, ...SelectResult<Arr>]
  : VT extends Column<infer X, infer Y>
  ? (true extends X ? null : never) | Y
  : VT extends { [key: string]: SqlViewTemplate }
  ? { -readonly [key in keyof VT]: SelectResult<VT[key]> }
  : never


