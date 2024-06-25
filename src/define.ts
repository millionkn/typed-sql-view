import { DeepTemplate, privateSym } from "./private.js"

export type AliasSym = object | Object

export type Segment = Array<string | AliasSym>

export type SqlState = 'leftJoin' | 'innerJoin' | 'where' | 'groupBy' | 'having' | 'take' | 'skip' | 'order'

export type Inner = {
  [privateSym]: 'inner'
  segment: Segment
}

type ColumnOpts<N extends boolean = boolean, R = unknown, T extends string = string> = {
  withNull: N,
  format: (raw: unknown) => R,
  tag: T,
  readonly inner: Inner,
}

export class Column<N extends boolean = boolean, R = unknown, T extends string = string> {

  private constructor(
    public opts: ColumnOpts<N, R, T>
  ) { }

  static [privateSym](inner: Inner): DefaultColumnType {
    return new Column({
      inner,
      format: (raw) => raw,
      tag: '',
      withNull: true,
    })
  }

  withNull<const N extends boolean>(value: N): Column<N, R, T> {
    return new Column({
      ...this.opts,
      withNull: value
    })
  }

  format<R2>(value: (raw: unknown, format: (raw: unknown) => R) => R2): Column<N, R2, T> {
    const format = this.opts.format
    return new Column({
      ...this.opts,
      format: (raw) => value(raw, format)
    })
  }

  assert<T1 extends T, T2 extends string = T1>(preTag: T1, curTag?: T2): Column<N, R, T2> {
    if (this.opts.tag !== preTag) {
      throw new Error(`assert tag '${preTag}',but saved is '${this.opts.tag}'`)
    }
    if (curTag === undefined || preTag as any === curTag) { return this as any }
    return new Column({
      ...this.opts,
      tag: curTag,
    })
  }
}

export type GetColumnHolder<T> = (ref: (e: T) => Column) => string

export type DefaultColumnType = Column<boolean, unknown, ''>

export type AnalysisResult = {
  usedInner: Inner[],
  order: boolean,
}

export type InitContext = {
  language: {
    skip: string,
    take: string,
  },
  genTableAlias: () => string,
  setParam: (value: any) => string,
}

export type BuildContext = {
  resolveAliasSym: (aliasSym: AliasSym) => string,
}


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

