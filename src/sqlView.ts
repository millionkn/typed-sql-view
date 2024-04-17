import { SqlBody } from "./sqlBody.js";
import { Column, ColumnDeclareFun, GetRefStr, InnerColumn, Relation, Resolvable, Segment, SqlState, SqlViewTemplate, exec, flatViewTemplate, getSegmentTarget, hasOneOf, pickConfig, resolveExpr, segmentToStr } from "./tools.js";

export class SqlView<VT1 extends SqlViewTemplate> {
  constructor(
    private createBuildCtx: () => {
      template: VT1,
      analysis: (ctx: {
        order: boolean,
        usedColumn: InnerColumn[]
      }) => SqlBody,
    },
  ) { }

  pipe = <R>(op: (self: this) => R): R => {
    return op(this)
  }

  andWhere = (getExpr: (tools: {
    ref: GetRefStr<VT1>,
    param: (value: any) => string,
    select1From: (view: SqlView<SqlViewTemplate>) => string,
  }) => null | false | undefined | string): SqlView<VT1> => {
    return new SqlView(() => {
      const buildCtx = this.createBuildCtx()
      const segment = resolveExpr<Resolvable | InnerColumn>((holder) => getExpr({
        ref: (ref) => holder(Column.getOpts(ref(buildCtx.template)).inner),
        param: (value) => holder((ctx) => ctx.setParam(value)),
        select1From: (view) => view
          .createBuildCtx()
          .analysis({ order: false, usedColumn: [] })
          .build(new Map([[() => `1`, '1']]), {
            genTableAlias: () => holder(exec(() => {
              let saved = ''
              return (ctx) => saved ||= ctx.genTableAlias()
            })),
            resolveSym: (sym) => holder((ctx) => ctx.resolveSym(sym)),
            setParam: (value) => holder(exec(() => {
              let saved = ''
              return (ctx) => saved ||= ctx.setParam(value)
            })),
            sym: {
              skip: holder((ctx) => ctx.sym.skip) as any,
              take: holder((ctx) => ctx.sym.take) as any,
            }
          }),
      }) || '')
      if (segment.length === 0) { return buildCtx }
      return {
        template: buildCtx.template,
        analysis: (ctx) => {
          const body = buildCtx.analysis({
            order: ctx.order,
            usedColumn: ctx.usedColumn.concat(getSegmentTarget(segment, (e) => e instanceof InnerColumn && e))
          }).bracketIf(ctx.usedColumn, ({ state }) => hasOneOf(state, ['skip', 'take']))
          const target = body.opts.groupBy.length === 0 ? body.opts.where : body.opts.having
          target.push((ctx) => segmentToStr(segment, (e) => e instanceof InnerColumn ? e.resolvable(ctx) : e(ctx)))
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
      const info = new Map<InnerColumn, Segment<InnerColumn>>()
      const keysTemplate = getKeyTemplate(buildCtx.template)
      const contentTemplate = getValueTemplate((columnExpr) => {
        const segment = resolveExpr<InnerColumn>((holder) => columnExpr((ref) => holder(Column.getOpts(ref(buildCtx.template)).inner)))
        const inner = new InnerColumn((ctx) => segmentToStr(segment, (inner) => inner.resolvable(ctx)))
        info.set(inner, segment)
        return Column.create(inner)
      })
      return {
        template: { keys: keysTemplate, content: contentTemplate },
        analysis: (ctx) => {
          const usedContentColumn = ctx.usedColumn.flatMap((inner) => getSegmentTarget(info.get(inner) || [], (v) => v))
          const keysColumn = flatViewTemplate(keysTemplate).map((c) => Column.getOpts(c).inner)
          const usedColumn = [...new Set(keysColumn.concat(usedContentColumn))]
          const body = buildCtx.analysis({
            order: false,
            usedColumn: usedColumn,
          }).bracketIf(usedColumn, ({ state }) => hasOneOf(state, ['order', 'groupBy', 'having', 'skip', 'take']))
          body.opts.groupBy = keysColumn.map((inner) => inner.resolvable)
          return body
        },
      }
    })
  }

  join = <
    M extends 'left' | 'inner' | 'lazy',
    N extends { inner: false, left: boolean, lazy: boolean }[M],
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
      const condation = resolveExpr<InnerColumn>((holder) => getCondation({
        ref: (ref) => holder(Column.getOpts(ref({
          base: base.template,
          extra: extra.template,
        })).inner),
      }))
      const extraColumnArr = [...new Set(flatViewTemplate(extra.template).map((c) => {
        const opts = Column.getOpts(c)
        opts.withNull ||= withNull
        return opts.inner
      }))]
      return {
        template: {
          base: base.template,
          extra: extra.template as Relation<N, VT2>,
        },
        analysis: (ctx) => {
          if (mode === 'lazy' && !extraColumnArr.find((inner) => ctx.usedColumn.includes(inner))) {
            return base.analysis(ctx)
          }
          const baseColumnArr = [...new Set(flatViewTemplate(base.template).map((c) => Column.getOpts(c).inner))]
          const usedBaseColumn = ctx.usedColumn.filter((inner) => baseColumnArr.includes(inner)).concat(getSegmentTarget(condation, (e) => baseColumnArr.includes(e) && e))
          const usedExtraColumn = ctx.usedColumn.filter((inner) => extraColumnArr.includes(inner)).concat(getSegmentTarget(condation, (e) => extraColumnArr.includes(e) && e))
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
                condation: (ctx) => segmentToStr(condation, (inner) => inner.resolvable(ctx)),
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

  mapTo = <const VT extends SqlViewTemplate>(getTemplate: (e: VT1, define: ColumnDeclareFun<(c: Column) => string>) => VT): SqlView<VT> => {
    return new SqlView(() => {
      const buildCtx = this.createBuildCtx()
      const info = new Map<InnerColumn, Segment<InnerColumn>>()
      const flat = (innerArr: InnerColumn[]): InnerColumn[] => innerArr.flatMap((inner) => {
        const segment = info.get(inner)
        if (!segment) { return [inner] }
        return flat(getSegmentTarget(segment, (inner) => inner))
      })
      return {
        template: getTemplate(buildCtx.template, (getExpr) => {
          const segment = resolveExpr<InnerColumn>((holder) => getExpr((c) => holder(Column.getOpts(c).inner)))
          const inner = new InnerColumn((ctx) => segmentToStr(segment, (inner) => inner.resolvable(ctx)))
          info.set(inner, segment)
          return Column.create(inner)
        }),
        analysis: (ctx) => buildCtx.analysis({
          order: ctx.order,
          usedColumn: flat(ctx.usedColumn),
        })
      }
    })
  }

  forceMapTo = <const VT extends SqlViewTemplate>(getTemplate: (e: VT1, define: ColumnDeclareFun<(c: Column) => string>) => VT): SqlView<VT> => {
    return new SqlView(() => {
      const createdColumn = new Array<Column>()
      const buildCtx = this
        .bracketIf(({ state }) => hasOneOf(state, ['groupBy', 'order', 'skip', 'take', 'where']))
        .mapTo((e, define) => getTemplate(e, (getExpr) => {
          const column = define(getExpr)
          createdColumn.push(column)
          return column
        }))
        .createBuildCtx()
      return {
        template: buildCtx.template,
        analysis: (ctx) => {
          const usedColumn = [...new Set(ctx.usedColumn.concat(createdColumn.map((e) => Column.getOpts(e).inner)))]
          return buildCtx.analysis({
            order: ctx.order,
            usedColumn,
          }).bracketIf(usedColumn, () => true)
        }
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
      const segment = resolveExpr<InnerColumn>((holder) => getExpr((ref) => holder(Column.getOpts(ref(buildCtx.template)).inner)))
      return {
        template: buildCtx.template,
        analysis: (ctx) => {
          if (!ctx.order) { return buildCtx.analysis(ctx) }
          const usedColumn = [...new Set(ctx.usedColumn.concat(getSegmentTarget(segment, (e) => e)))]
          const sqlBody = buildCtx.analysis({
            order: ctx.order,
            usedColumn,
          }).bracketIf(usedColumn, ({ state }) => hasOneOf(state, ['skip', 'take']))
          sqlBody.opts.order.unshift({
            order,
            resolvable: (ctx) => segmentToStr(segment, (inner) => inner.resolvable(ctx)),
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

  static createBuildCtx<VT1 extends SqlViewTemplate>(view: SqlView<VT1>) {
    return view.createBuildCtx()
  }
};