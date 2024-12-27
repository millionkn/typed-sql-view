export const sym = Symbol()

export const exec = <T>(fun: () => T): T => fun()

export type DeepTemplate<I> = I | (readonly [...DeepTemplate<I>[]]) | { readonly [key: string]: DeepTemplate<I> }

export function flatViewTemplate<T extends string>(template: SqlViewTemplate<T>): Column<T, unknown, boolean>[] {
  if (template instanceof Column) { return [template] }
  if (template instanceof Array) { return template.flatMap((e) => flatViewTemplate(e)) }
  return Object.values(template).flatMap((e) => flatViewTemplate(e))
}

export type Inner = {
  declareUsed: () => {
    ref: string,
  },
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
    private opts: {
      inner: Inner,
      withNull: N,
      format: (raw: unknown) => R,
      assert: T,
    }
  ) { }

  [sym]() {
    if (this.opts.assert !== '') {
      throw new Error(`column should assert '${this.opts.assert}'`)
    }
    return this.opts
  }


  withNull<const N extends boolean>(value: N) {
    return new Column<T, R, N>({
      ...this.opts,
      withNull: value,
    })
  }

  format<R2>(value: (raw: unknown, format: (raw: unknown) => R) => R2) {
    const format = this.opts.format
    return new Column<T, R2, N>({
      ...this.opts,
      format: (raw) => value(raw, format)
    })
  }

  assert<T2 extends string>(pre: T, cur: T2) {
    if (this.opts.assert !== pre) {
      throw new Error(`assert tag '${pre}',but saved is '${this.opts.assert}'`)
    }
    return new Column<T2, R, N>({
      ...this.opts,
      assert: cur,
    })
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

export type BuildCtx = {
  genAlias: () => string,
  setParam: (value: unknown) => string,
}
