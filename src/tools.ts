export class Column<N extends boolean = boolean, R = unknown> {
  constructor(
    private opts: {
      withNull: N,
      format: (raw: unknown) => R,
    }
  ) { }

  static getOpts<N extends boolean, R>(column: Column<N, R>) {
    return column.opts
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

export type SqlState = 'leftJoin' | 'innerJoin' | 'where' | 'groupBy' | 'having' | 'take' | 'skip' | 'order'
