import { SqlBody } from "./sqlBody.js"

export const privateSym = Symbol()

export class Adapter<PC = unknown> {
  static mysqlAdapter = new Adapter({
    skip: 'skip',
    take: 'take',
    createParamCtx: () => {
      const result: { [key: string]: unknown } = {}
      return {
        getParamResult: () => result,
        setParam: (value, index) => {
          result[`param${index}`] = value
          return { holder: `:param${index}` }
        },
      }
    },
  })

  static postgresAdapter = new Adapter({
    skip: 'offset',
    take: 'limit',
    createParamCtx: () => {
      const result: unknown[] = []
      return {
        getParamResult: () => result,
        setParam: (value, index) => {
          result.push(value)
          return { holder: `$${index + 1}` }
        },
      }
    },
  })

  constructor(
    public opts: {
      createParamCtx: () => {
        getParamResult: () => PC,
        setParam: (value: unknown, index: number) => { holder: string }
      },
      skip: 'skip' | 'offset',
      take: 'take' | 'limit',
    }
  ) { }

  createBuildCtx(): BuildContext {
    return {
      resolveSym: () => { throw new Error() },
      param: exec(() => {
        let index = 0
        const paramCtx = this.opts.createParamCtx()
        return {
          getResult: () => paramCtx.getParamResult(),
          set: (v) => paramCtx.setParam(v, index++).holder
        }
      }),
      genTableAlias: exec(() => {
        let index = 0
        return () => `table_${index++}`
      }),
      language: {
        skip: this.opts.skip,
        take: this.opts.take,
      }
    }
  }
}

export type InitContext = {
  resolveSegmentStr: (getStr: (holder: (value: () => string) => string) => string) => Expr<V>,
  createColumn: ColumnDeclareFun<(c: Column) => string>,
}

export type AnalysisResult = {
  order: boolean
}

export type AnalysisContext = AnalysisResult & {
  usedColumn: Column[],
}

export type TransformResult<VT extends SqlViewTemplate> = {
  template: VT,
  analysis: (ctx: AnalysisContext) => SqlBody,
}

export type TransformContext<VT extends SqlViewTemplate> = {
  appendUsedColumn: (c: Column) => void,
  template: VT,
  analysis: (ctx: AnalysisResult) => SqlBody,
}

export type ExprContext = {
  resolveSym: (sym: object) => string,
}
export type ExprResolver = (ctx: ExprContext) => string

export type BuildContext = ExprContext & {
  param: {
    set: (value: unknown) => string,
    getResult: () => unknown,
  },
  genTableAlias: () => string,
  language: {
    skip: string,
    take: string,
  }
}
export type SegmentResolver = (ctx: BuildContext) => string


type DefaultColumnType = Column<boolean, unknown, ''>
export type ColumnDeclareFun<I> = (columnExpr: (input: I) => string) => DefaultColumnType
export class Column<N extends boolean = boolean, R = unknown, T extends string = string> {
  private constructor(
    private opts: {
      withNull: boolean,
      format: (raw: unknown) => R,
      tag: T,
      inner: {
        exprResolver: ExprResolver,
      }
    }
  ) { }
  static [privateSym] = {
    getOpts: (column: Column) => column.opts,
    create: (resolvable: ExprResolver): DefaultColumnType => new Column({
      withNull: true,
      format: () => { throw new Error() },
      tag: '',
      inner: {
        exprResolver: resolvable,
      }
    }),
  }
  readonly withNull = <const N extends boolean>(value: N): Column<N, R, T> => {
    return new Column({
      ...this.opts,
      withNull: value
    })
  }

  readonly format = <R2>(value: (raw: unknown, format: (raw: unknown) => R) => R2): Column<N, R2, T> => {
    return new Column({
      ...this.opts,
      format: (raw) => value(raw, this.opts.format)
    })
  }

  readonly assert = <T1 extends T, T2 extends string = T1>(preTag: T1, curTag?: T2): Column<N, R, T2> => {
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

export type DeepTemplate<I> = I | (readonly DeepTemplate<I>[]) | { readonly [key: string]: DeepTemplate<I> }
export type SqlViewTemplate = DeepTemplate<Column>

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

export type SqlState = 'leftJoin' | 'innerJoin' | 'where' | 'groupBy' | 'having' | 'take' | 'skip' | 'order'


export const exec = <T>(fun: () => T): T => fun()

export function hasOneOf<T>(items: Iterable<T>, arr: (T & {})[]) {
  return !![...items].find((e) => arr.includes(e as any))
}

export function pickConfig<K extends string | number, R>(key: K, config: { [key in K]: () => R }): R {
  return config[key]()
}

function _flatViewTemplate(template: SqlViewTemplate): Column[] {
  if (template instanceof Column) { return [template] }
  if (template instanceof Array) { return template.flatMap((e) => _flatViewTemplate(e)) }
  return Object.values(template).flatMap((e) => _flatViewTemplate(e))
}

export function flatViewTemplate(template: SqlViewTemplate): Set<Column> {
  return new Set(_flatViewTemplate(template))
}
