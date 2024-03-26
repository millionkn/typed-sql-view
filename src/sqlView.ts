import { SqlAdapter } from "./adapter.js";
import { SqlBody } from "./sqlBody.js";
import { Column, ColumnDeclareFun, DeepTemplate, GetRefStr, Relation, Segment, SqlState, SqlViewTemplate, flatViewTemplate, getSegmentTarget, hasOneOf, pickConfig, resolveExpr } from "./tools.js";

export class SqlView<VT1 extends SqlViewTemplate> {
  constructor(
    private createBuildCtx: () => {
      template: VT1,
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
    selectFrom: (view: SqlView<SqlViewTemplate>) => string,
  }) => null | false | undefined | string): SqlView<VT1> => {
    return new SqlView(() => {
      const buildCtx = this.createBuildCtx()
      return {
        template: buildCtx.template,
        analysis: (ctx) => {
          const body = buildCtx.analysis(ctx).bracketIf(ctx.usedColumn, ({ state }) => hasOneOf(state, ['skip', 'take']))
          const target = body.opts.groupBy.length === 0 ? body.opts.where : body.opts.having
          target.push((ctx) => getExpr({
            ref: (ref) => Column.getResolvable(ref(buildCtx.template))(ctx),
            param: (value) => ctx.setParam(value),
            selectFrom: (view) => {
              throw 'todo'
            },
          }) || '')
          return body
        },
      }
    })
  }

  groupBy = <const K extends SqlViewTemplate, const VT extends SqlViewTemplate>(
    getKeyTemplate: (vt: VT1) => K,
    getValueTemplate: (
      define: ColumnDeclareFun<GetRefStr<VT1>>,
    ) => VT,
  ): SqlView<{ keys: K, content: VT }> => {
    return new SqlView(() => {
      const buildCtx = this.createBuildCtx()
      const info = new Map<Column, { segment: Segment<Column> }>()
      const keysTemplate = getKeyTemplate(buildCtx.template)
      const contentTemplate = getValueTemplate((withNull, columnExpr, formatter) => {
        const column = new Column<any, any>()
        info.set(column, { segment: resolveExpr((holder) => columnExpr((ref) => holder(ref(buildCtx.template)))) })
        return column
      })
      return {
        template: { keys: keysTemplate, content: contentTemplate },
        analysis: (ctx) => {
          const usedContentColumn = ctx.usedColumn.flatMap((c) => getSegmentTarget(info.get(c)?.segment || []))
          const keysColumn = flatViewTemplate(keysTemplate)
          const usedColumn = [...new Set(keysColumn.concat(usedContentColumn))]
          const body = buildCtx.analysis({
            order: ctx.order,
            usedColumn: usedColumn,
          }).bracketIf(usedColumn, ({ state }) => hasOneOf(state, ['groupBy', 'having', 'skip', 'take']))
          body.opts.groupBy = keysColumn.map((column) => (ctx) => Column.getResolvable(column)(ctx))
          usedContentColumn.forEach((c) => Column.setResolvable(c, (ctx) => info.get(c)!.segment.map((e) => typeof e === 'string' ? e : Column.getResolvable(e.value)(ctx)).join('')))
          return body
        },
      }
    })
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
      const base = this.createBuildCtx()
      const extra = view.createBuildCtx()
      return {
        template: {
          base: base.template,
          extra: extra.template as Relation<N, VT2>,
        },
        analysis: (ctx) => {
          const extraColumnArr = flatViewTemplate(extra.template)
          if (mode === 'lazy' && !ctx.usedColumn.find((e) => extraColumnArr.includes(e))) {
            return base.analysis(ctx)
          }
          const baseColumnArr = flatViewTemplate(base.template)
          const condation = resolveExpr<Column>((holder) => getCondation({
            ref: (ref) => holder(ref({
              base: base.template,
              extra: extra.template,
            })),
          }))
          const usedBaseColumn = ctx.usedColumn.filter((e) => baseColumnArr.includes(e)).concat(getSegmentTarget(condation).filter((e) => baseColumnArr.includes(e)))
          const usedExtraColumn = ctx.usedColumn.filter((e) => extraColumnArr.includes(e)).concat(getSegmentTarget(condation).filter((e) => extraColumnArr.includes(e)))
          let baseBody = base.analysis({
            order: ctx.order,
            usedColumn: usedBaseColumn,
          }).bracketIf(usedBaseColumn, ({ state }) => hasOneOf(state, ['groupBy', 'having', 'order', 'skip', 'take']))
          let extraBody = extra.analysis({
            usedColumn: usedExtraColumn,
            order: pickConfig(mode satisfies "inner" | "left" | "lazy", {
              'lazy': () => false,
              'left': () => ctx.order,
              'inner': () => ctx.order,
            }),
          }).bracketIf(usedExtraColumn, ({ state }) => {
            return pickConfig(mode satisfies "inner" | "left" | "lazy", {
              lazy: () => hasOneOf(state, ['innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
              left: () => hasOneOf(state, ['innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
              inner: () => hasOneOf(state, ['leftJoin', 'innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
            })
          })
          return new SqlBody({
            from: baseBody.opts.from,
            join: [
              ...baseBody.opts.join ?? [],
              {
                type: mode === 'inner' ? 'inner' : 'left',
                aliasSym: extraBody.opts.from.aliasSym,
                resolvable: extraBody.opts.from.resolvable,
                condation: (ctx) => condation.map((e) => typeof e === 'string' ? e : Column.getResolvable(e.value)(ctx)).join(''),
              },
              ...extraBody.opts.join ?? [],
            ],
            where: [...baseBody.opts.where ?? [], ...extraBody.opts.where ?? []],
            groupBy: [],
            having: [],
            order: baseBody.opts.order,
            take: baseBody.opts.take,
            skip: baseBody.opts.skip,
          })
        },
      }
    })
  }

  mapTo = <VT extends SqlViewTemplate>(getTemplate: (e: VT1, define: ColumnDeclareFun<(c: Column) => string>) => VT): SqlView<VT> => {
    return new SqlView(() => {
      const buildCtx = this.createBuildCtx()
      const info = new Map<Column, Segment<Column>>()
      const flat = (columnArr: Column[]): Column[] => columnArr.flatMap((c) => {
        const segment = info.get(c)
        if (!segment) { return [c] }
        return flat(getSegmentTarget(segment))
      })
      return {
        template: getTemplate(buildCtx.template, (withNull, getExpr, formatter) => {
          const column = new Column<any, any>()
          info.set(column, resolveExpr((holder) => getExpr((c) => holder(c))))
          return column
        }),
        analysis: (ctx) => buildCtx.analysis({
          order: ctx.order,
          usedColumn: flat(ctx.usedColumn),
        })
      }
    })
  }

  bracketIf = (condation: (opts: {
    state: SqlState[],
  }) => boolean): SqlView<VT1> => {
    return new SqlView(() => {
      const buildCtx = this.createBuildCtx()
      return {
        template: buildCtx.template,
        analysis: (ctx) => buildCtx.analysis(ctx).bracketIf(ctx.usedColumn, condation),
      }
    })
  }

  order = (
    order: 'asc' | 'desc',
    getExpr: (ref: GetRefStr<VT1>) => string,
  ): SqlView<VT1> => {
    return new SqlView(() => {
      const buildCtx = this.createBuildCtx()
      return {
        template: buildCtx.template,
        analysis: (ctx) => {
          if (!ctx.order) { return buildCtx.analysis(ctx) }
          const segment = resolveExpr<Column>((holder) => getExpr((ref) => holder(ref(buildCtx.template))))
          const usedColumn = [...new Set(ctx.usedColumn.concat(getSegmentTarget(segment)))]
          const sqlBody = buildCtx.analysis({
            order: ctx.order,
            usedColumn,
          }).bracketIf(usedColumn, ({ state }) => hasOneOf(state, ['skip', 'take']))
          sqlBody.opts.order.unshift({
            order,
            resolvable: (ctx) => segment.map((e) => typeof e === 'string' ? e : Column.getResolvable(e.value)(ctx)).join(''),
          })
          return sqlBody
        },
      }
    })
  }

  take = (count: number | null | undefined | false): SqlView<VT1> => {
    if (typeof count !== 'number') { return this }
    return new SqlView(() => {
      const buildCtx = this.createBuildCtx()
      return {
        template: buildCtx.template,
        analysis: (ctx) => {
          const sqlBody = buildCtx.analysis({
            order: true,
            usedColumn: ctx.usedColumn,
          })
          sqlBody.opts.take = sqlBody.opts.take === null ? count : Math.min(sqlBody.opts.take, count)
          return sqlBody
        },
      }
    })
  }

  skip = (count: number | null | undefined | false): SqlView<VT1> => {
    if (typeof count !== 'number') { return this }
    return new SqlView(() => {
      const buildCtx = this.createBuildCtx()
      return {
        template: buildCtx.template,
        analysis: (ctx) => {
          const sqlBody = buildCtx.analysis({
            order: true,
            usedColumn: ctx.usedColumn,
          }).bracketIf(ctx.usedColumn, ({ state }) => hasOneOf(state, ['take']))
          sqlBody.opts.skip = sqlBody.opts.skip + count
          sqlBody.opts.take = sqlBody.opts.take === null ? null : Math.max(0, sqlBody.opts.take - count)
          return sqlBody
        },
      }
    })
  }

  buildSelect<ST extends { [key: string]: Column }>(
    adapter: SqlAdapter,
    getTemplate: (createSelect: CreateSelect<VT1>) => ST,
  ) {

  }
};