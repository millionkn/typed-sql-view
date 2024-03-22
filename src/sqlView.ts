import { ColumnDeclareFun, GetRefStr, Relation, SqlState, SqlViewTemplate } from "./tools.js";

const columnProxySym = Symbol()

export function createFromDefine<VT extends SqlViewTemplate>(
  from: string,
  getTemplate: (define: ColumnDeclareFun<string>) => VT
) {
  const template = getTemplate((withNull, columnExpr, formatter) => new Proxy({} as any, {
    has: (target, p) => {
      return p === columnProxySym || p in target
    },
    get: (target, key) => {
      if (key !== columnProxySym) { return }
      return { withNull, columnExpr, formatter }
    },
  }))
  return new SqlView({
    template,
  })
}

export type SqlViewOpts<VT1 extends SqlViewTemplate> = {
  template: VT1,
  // analysis: (ctx: AnalysisContext) => {
  //   output: ForceMap<Column, Resolvable>,
  //   body: SqlBody,
  // },
}

export class SqlView<VT1 extends SqlViewTemplate> {
  constructor(
    private opts: SqlViewOpts<VT1>,
  ) { }

  pipe = <R>(op: (self: this) => R): R => {
    return op(this)
  }

  andWhere = (getExpr: (tools: {
    ref: GetRefStr<VT1>,
    param: (value: any) => string,
    exists: (view: SqlView<SqlViewTemplate>) => string,
  }) => null | false | undefined | string): SqlView<VT1> => {
    throw 'todo'
  }

  groupBy = <const K extends SqlViewTemplate, const VT extends SqlViewTemplate>(
    getKeyTemplate: (vt: VT1) => K,
    getTemplate: (
      define: ColumnDeclareFun<GetRefStr<VT1>>,
    ) => VT,
  ): SqlView<{ keys: K, content: VT }> => {
    throw 'todo'
  }

  join = <
    M extends 'left' | 'inner' | 'lazy',
    N extends (M extends 'inner' ? false : boolean),
    VT2 extends SqlViewTemplate,
  >(
    mode: M,
    withNull: N,
    view: SqlView<VT2>,
    getCondation: (tools: {
      baseRef: GetRefStr<VT1>,
      extraRef: GetRefStr<VT2>,
    }) => string,
  ): SqlView<{ base: VT1, extra: Relation<N, VT2> }> => {
    throw 'todo'
  }

  mapTo = <VT extends SqlViewTemplate>(getTemplate: (e: VT1) => VT): SqlView<VT> => {
    throw 'todo'
  }

  bracketIf = (useWrap: (opts: {
    state: SqlState[],
  }) => boolean): SqlView<VT1> => {
    throw 'todo'
  }

  order = (
    mode: 'push' | 'unshift',
    order: 'asc' | 'desc',
    expr: (ref: GetRefStr<VT1>) => string,
  ): SqlView<VT1> => {
    throw 'todo'
  }

  take = (count: number | null | undefined | false): SqlView<VT1> => {
    throw 'todo'
  }

  skip = (count: number | null | undefined | false): SqlView<VT1> => {
    throw 'todo'
  }
};