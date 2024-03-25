import { bracketIf } from "./bracketIf.js";
import { SqlBody } from "./sqlBody.js";
import { Column, ColumnDeclareFun, GetRefStr, Relation, SqlState, SqlViewTemplate, hasOneOf, pickConfig, resolveExpr } from "./tools.js";

export class SqlView<VT1 extends SqlViewTemplate> {
  constructor(
    private buildTemplate: () => {
      template: VT1,
      columnArr: Column[],
      analysis: (ctx: {
        order: boolean,
        usedColumn: Column[]
      }) => SqlBody,
    },
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
      ref: GetRefStr<{ base: VT1, extra: VT2 }>,
    }) => string,
  ): SqlView<{ base: VT1, extra: Relation<N, VT2> }> => {
    return new SqlView(() => {
      const base = this.buildTemplate()
      const extra = view.buildTemplate()
      return {
        template: {
          base: base.template,
          extra: extra.template as any,
        },
        columnArr: base.columnArr.concat(extra.columnArr),
        analysis: (ctx) => {
          if (mode === 'lazy' && !ctx.usedColumn.find((e) => extra.columnArr.includes(e))) {
            return base.analysis(ctx)
          }
          const condation = resolveExpr<Column>((holder) => getCondation({
            ref: (ref) => holder(ref({
              base: base.template,
              extra: extra.template,
            })),
          }))
          const usedBaseColumn = ctx.usedColumn.filter((e) => base.columnArr.includes(e)).concat(condation
            .map((e) => typeof e === 'object' && base.columnArr.includes(e.value) && e.value)
            .filter((e): e is Column => !!e)
          )
          const usedExtraColumn = ctx.usedColumn.filter((e) => extra.columnArr.includes(e)).concat(condation
            .map((e) => typeof e === 'object' && extra.columnArr.includes(e.value) && e.value)
            .filter((e): e is Column => !!e)
          )

          const baseBody = bracketIf({
            usedColumn: usedBaseColumn,
            sqlBody: base.analysis({
              order: ctx.order,
              usedColumn: usedBaseColumn,
            }),
            useWrap: ({ state }) => {
              return pickConfig(mode satisfies "inner" | "left" | "lazy", {
                'left': () => hasOneOf(state, ['where', 'groupBy', 'having', 'order', 'skip', 'take']),
                'lazy': () => hasOneOf(state, ['where', 'leftJoin', 'innerJoin', 'groupBy', 'having', 'skip', 'take']),
                'inner': () => hasOneOf(state, ['groupBy', 'having', 'order', 'skip', 'take']),
              })
            }
          })
          const extraBody = bracketIf(usedExtraColumn, extra.analysis({
            order: pickConfig(mode satisfies "inner" | "left" | "lazy", {
              'lazy': () => false,
              'left': () => ctx.order,
              'inner': () => ctx.order,
            }),
            usedColumn: usedExtraColumn.concat(usedExtraColumn),
          }), ({ state }) => {

          })

        },
      }
    })
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