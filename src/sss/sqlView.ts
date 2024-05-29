import { SqlBody } from "./sqlBody.js";
import { TransformContext, TransformResult, SqlState, SqlViewTemplate, hasOneOf, flatViewTemplate, Column, ColumnDeclareFun, InitContext, GetRefStr, Relation, exec, privateSym, pickConfig, BuildContext, Adapter } from "./tools.js";

export const createInitContext = (): InitContext => {
  const resolveExprStr = exec((): InitContext['resolveSegmentStr'] => {
    let _nsIndex = 0
    return <V>(getExpr: (holder: (value: V) => string) => string) => {
      const nsIndex = _nsIndex += 1
      const split = `'"\`${nsIndex}'"\``
      let index = 0
      const saved = new Map<string, V>()
      const expr = getExpr((c) => {
        const key = `holder_${nsIndex}_${index += 1}`
        saved.set(key, c)
        return `${split}${key}${split}`
      })
      _nsIndex -= 1
      return expr.length === 0 ? [] : expr.split(split).map((str, i) => {
        if (i % 2 === 0) { return str }
        const value = saved.get(str)
        if (!value) { throw new Error() }
        return { value }
      })
    }
  })
  return {
    resolveSegmentStr: resolveExprStr,
    createColumn: (getExpr) => {
      const expr = resolveExprStr<InnerColumn>((holder) => getExpr((c) => holder(c[privateSym].inner)))
      const inner = InnerColumn[privateSym].create({
        deps: new Set(expr.filter((e): e is { value: InnerColumn } => typeof e === 'object').map((e) => e.value)),
        resolvable: (ctx) => expr.map((e) => typeof e === 'string' ? e : e.value.opts.resolvable(ctx)).join('')
      })
      return Column[privateSym].create(inner)
    },
  }
}

export const createTransformContext = <VT1 extends SqlViewTemplate>(view: SqlView<VT1>, init: InitContext): TransformContext<VT1> => {
  let afterTransform = false
  const { template, analysis } = view.getTransformResult(init)
  const appendUsedColumn = new Set<Column>();
  return {
    template,
    analysis: (ctx) => {
      afterTransform = true
      return analysis({
        flag: ctx.flag,
        usedColumn: [...new Set(appendUsedColumn)],
      })
    },
    appendUsedColumn: (c) => {
      if (afterTransform) { throw new Error() }
      appendUsedColumn.add(c)
    },
  }
}

export const createBaseBuildCtx = (adapter: Adapter): BuildContext => {
  return {
    resolveSym: () => { throw new Error() },
    setParam: exec(() => {
      let index = 0
      const paramCtx = adapter.createParamCtx()
      return (v) => paramCtx.setParam(v, index++).holder
    }),
    genTableAlias: exec(() => {
      let index = 0
      return () => `table_${index++}`
    }),
    language: {
      skip: adapter.skip,
      take: adapter.take,
    }
  }
}

export class SqlView<VT1 extends SqlViewTemplate> {
  constructor(
    public getTransformResult: (initContext: InitContext) => TransformResult<VT1>,
  ) { }

  pipe = <R>(op: (self: this) => R): R => op(this)

  bracketIf = (
    condation: (opts: { state: SqlState[] }) => boolean
  ) => new SqlView((init) => {
    const ctx = createTransformContext(this, init)
    return {
      template: ctx.template,
      analysis: (_ctx) => {
        const sqlBody = ctx.analysis(_ctx)
        if (condation({ state: [...sqlBody.state()] })) {
          return sqlBody.bracket(_ctx.usedColumn)
        } else {
          return sqlBody
        }
      },
    }
  })

  mapTo = <const VT extends SqlViewTemplate>(getTemplate: (e: VT1, define: ColumnDeclareFun<(c: Column) => string>) => VT): SqlView<VT> => {
    return new SqlView((init) => {
      const ctx = createTransformContext(this, init)
      return {
        template: getTemplate(ctx.template, init.createColumn),
        analysis: ctx.analysis,
      }
    })
  }

  forceMapTo = <const VT extends SqlViewTemplate>(getTemplate: (e: VT1, define: ColumnDeclareFun<(c: Column) => string>) => VT): SqlView<VT> => {
    return new SqlView((init) => {
      const ctx = this
        .bracketIf(({ state }) => hasOneOf(state, ['groupBy', 'order', 'skip', 'take', 'where']))
        .pipe((view) => createTransformContext(view, init))
      const template = getTemplate(ctx.template, init.createColumn)
      flatViewTemplate(template).forEach((c) => ctx.appendUsedColumn(c))
      return {
        template,
        analysis: ctx.analysis,
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
    getCondationExprStr: (tools: {
      ref: GetRefStr<{ base: VT1, extra: VT2 }>,
    }) => string,
  ): SqlView<{ base: VT1, extra: Relation<N, VT2> }> => {
    return new SqlView((init) => {
      const base = createTransformContext(this, init)
      const extra = createTransformContext(view, init)
      const condationExpr = init.resolveSegmentStr((holder) => getCondationExprStr({
        ref: (ref) => holder(ref({
          base: base.template,
          extra: extra.template,
        })),
      }))
      const condationInnerSet = filterInnerColumn(condationExpr)
      const extraInner = exec(() => {
        const extraColumnArr = flatViewTemplate(extra.template)
        extraColumnArr.forEach((c) => c[privateSym].withNull ||= withNull)
        const arr = [...extraColumnArr].map((c) => c[privateSym].inner)
        return { arr, set: new Set(arr) }
      })
      const baseInner = exec(() => {
        const arr = [...flatViewTemplate(base.template)].map((c) => c[privateSym].inner)
        return { arr, set: new Set(arr) }
      })
      return {
        template: {
          base: base.template,
          extra: extra.template as Relation<N, VT2>,
        },
        analysis: (ctx) => {
          if (mode === 'lazy') {
            if (!extraInner.arr.find((inner) => ctx.usedColumn.has(inner))) {
              return base.analysis(ctx)
            }
            if (withNull === false && !baseInner.arr.find((inner) => ctx.usedColumn.has(inner))) {
              return extra.analysis(ctx)
            }
          }
          const usedBaseColumn = new Set(baseInner.arr.filter((ic) => ctx.usedColumn.has(ic) || condationInnerSet.has(ic)))
          const usedExtraColumn = new Set(extraInner.arr.filter((ic) => ctx.usedColumn.has(ic) || condationInnerSet.has(ic)))

          let baseBody = base.analysis({
            flag: ctx.flag,
            usedColumn: usedBaseColumn,
          })
          if (hasOneOf(baseBody.state(), ['groupBy', 'having', 'order', 'skip', 'take'])) {
            baseBody.bracket(usedBaseColumn)
          }
          let extraBody = extra.analysis({
            usedColumn: usedExtraColumn,
            flag: {
              order: pickConfig(mode satisfies "inner" | "left" | "lazy", {
                'lazy': () => false,
                'left': () => ctx.flag.order,
                'inner': () => ctx.flag.order,
              }),
            }
          })
          pickConfig(mode satisfies "inner" | "left" | "lazy", {
            lazy: () => hasOneOf(extraBody.state(), ['innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
            left: () => hasOneOf(extraBody.state(), ['innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
            inner: () => hasOneOf(extraBody.state(), ['leftJoin', 'innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
          }) && extraBody.bracket(usedExtraColumn)
          return new SqlBody({
            from: baseBody.opts.from,
            join: [
              ...baseBody.opts.join ?? [],
              {
                type: mode === 'inner' ? 'inner' : 'left',
                aliasSym: extraBody.opts.from.aliasSym,
                segment: extraBody.opts.from.segment,
                condation: (ctx) => exprToStr(condationExpr, ctx),
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

  andWhere = (getSegmentStr: (holder: {
    ref: GetRefStr<VT1>,
    param: (value: any) => string,
    ctx: BuildContext,
  }) => null | false | undefined | string): SqlView<VT1> => {
    return new SqlView((init) => {

      this.andWhere(({ ref, param, ctx: buildCtx }) => `exists (${this})`)
      const ctx = createTransformContext(this, init)

      const expr = init.resolveSegmentStr((holder) => getSegmentStr({
        ref: (ref) => holder(ref(ctx.template)),
        param: (value) => holder((ctx) => ctx.setParam(value)),
      }) || '')
      if (expr.length === 0) { return ctx }

      return {
        template: buildCtx.template,
        analysis: (ctx) => {
          const body = buildCtx.analysis({
            order: ctx.order,
            usedColumn: ctx.usedColumn.concat(getSegmentTarget(expr, (e) => e instanceof InnerColumn && e))
          }).bracketIf(ctx.usedColumn, ({ state }) => hasOneOf(state, ['skip', 'take']))
          const target = body.opts.groupBy.length === 0 ? body.opts.where : body.opts.having
          target.push((ctx) => segmentToStr(expr, (e) => e instanceof InnerColumn ? e.resolvable(ctx) : e(ctx)))
          return body
        },
      }
    })
  }
};