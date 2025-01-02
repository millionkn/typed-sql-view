import { BuildCtx, GetColumnRef, Inner, Relation, SqlBody, SqlViewTemplate, buildSqlBodyStr, exec, flatViewTemplate, hasOneOf, pickConfig, sqlBodyState, sym } from "./tools.js"

export type BuildFlag = {
  order: boolean,
}

export type SelectRuntime<VT extends SqlViewTemplate<string>> = {
  template: VT,
  getSqlBody: (flag: BuildFlag, usedInner: Set<Inner>) => SqlBody,
}


export class SqlView<VT1 extends SqlViewTemplate<string>> {
  constructor(
     _getInstance: (buildCtx: BuildCtx) => SelectRuntime<VT1>,
  ) { 
    this.
  }

  pipe<R>(op: (self: this) => R): R {
    return op(this)
  }

  private _join<N extends boolean, VT2 extends SqlViewTemplate<string>>(
    mode: "left" | "inner" | "lazy",
    withNull: N,
    view: SqlView<VT2>,
    getCondationExpr: (ref: GetColumnRef<{ base: VT1, extra: VT2 }>) => string,
  ): SqlView<{ base: VT1, extra: Relation<N, VT2> }> {
    return new SqlView((ctx) => {
      const sp = exec(() => {
        const base = this._getInstance(ctx)
        const extra = view._getInstance(ctx)
        const info: Map<Inner, {
          from: 'base' | 'extra',
          preExpr: string,
          holder: ReturnType<typeof ctx.createHolder>,
        }> = new Map()

        flatViewTemplate(base.template).forEach((c) => {
          const inner = c[sym].inner
          const holder = ctx.createHolder()
          info.set(inner, {
            from: 'base',
            preExpr: inner.expr,
            holder,
          })
          inner.expr = holder.expr
        })
        const allExtra = flatViewTemplate(extra.template)
        allExtra.forEach((c) => {
          if (withNull) { c[sym].withNull = true }
          const inner = c[sym].inner
          const holder = ctx.createHolder()
          info.set(inner, {
            from: 'extra',
            preExpr: inner.expr,
            holder,
          })
          inner.expr = holder.expr
        })

        const condationExpr = exec(() => {
          const resolver = ctx.createResolver<Inner>()
          return resolver.resolve(getCondationExpr((ref) => resolver.createHolder(() => ref({
            base: base.template,
            extra: extra.template,
          })[sym].inner)))
        })
        return {
          info,
          base,
          extra,
          condationExpr,
        }
      })

      return {
        template: {
          base: sp.base.template,
          extra: sp.extra.template as Relation<N, VT2>,
        },
        getSqlBody: (flag, usedInner) => {
          const usedExtraInner = [...usedInner].filter((inner) => sp.info.get(inner)?.from === 'extra')
          const usedBaseInner = [...usedInner].filter((inner) => sp.info.get(inner)?.from === 'base')
          if (mode === 'lazy') {
            if (usedExtraInner.length === 0) {
              usedInner.forEach((inner) => {
                const info = sp.info.get(inner)
                info?.holder.replaceWith(info.preExpr)
              })
              return sp.base.getSqlBody(flag, usedInner)
            }
            if (!withNull && !flag.order) {
              if (usedBaseInner.length === 0) {
                usedInner.forEach((inner) => {
                  const info = sp.info.get(inner)
                  info?.holder.replaceWith(info.preExpr)
                })
                return sp.extra.getSqlBody(flag, usedInner)
              }
            }
          }
          const innerInCondation = sp.condationExpr.map((e) => typeof e === 'string' ? e : e()).filter((e): e is Inner => typeof e !== 'string')
          const baseBody = exec(() => {
            let sqlBody = sp.base.getSqlBody(
              {
                order: flag.order,
              },
              new Set(usedBaseInner.concat(innerInCondation.filter((e) => sp.info.get(e)?.from === 'base')))
            )
            const bracket = hasOneOf(sqlBodyState(sqlBody), ['groupBy', 'having', 'order', 'skip', 'take'])
            if (bracket) { sqlBody = sqlBody.bracket() }
            return sqlBody
          })
          let extraBody = exec(() => {
            let extraBody = sp.extra.getSqlBody(
              {
                order: pickConfig(mode, {
                  'lazy': () => false,
                  'left': () => flag.order,
                  'inner': () => flag.order,
                }),
              },
              new Set(usedBaseInner.concat(innerInCondation.filter((e) => sp.info.get(e)?.from === 'base')))
            )
            const extraState = sqlBodyState(extraBody)
            const bracket = pickConfig(mode, {
              lazy: () => hasOneOf(extraState, ['innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
              left: () => hasOneOf(extraState, ['innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
              inner: () => hasOneOf(extraState, ['leftJoin', 'innerJoin', 'groupBy', 'having', 'order', 'skip', 'take']),
            })
            if (bracket) { extraBody = extraBody.bracket() }
            return extraBody
          })


          return {
            from: baseBody.from,
            join: [
              ...baseBody.join ?? [],
              ...extraBody.from.map((info, index) => {
                return {
                  type: index === 0 ? 'inner' as const : pickConfig(mode, {
                    left: () => 'left' as const,
                    inner: () => 'inner' as const,
                    lazy: () => 'left' as const,
                  }),
                  alias: info.alias,
                  expr: info.expr,
                  condation: sp.condationExpr.flatMap((e) => typeof e === 'string' ? e : e().declareUsed().ref).join(''),
                }
              }),
              ...extraBody.join ?? [],
            ],
            where: [...baseBody.where ?? [], ...extraBody.where ?? []],
            groupBy: [],
            having: [],
            order: baseBody.order,
            take: baseBody.take,
            skip: baseBody.skip,
          }
        },
      }
    })
  }

  rawBuild(flag: BuildFlag, buildCtx: BuildCtx) {
    const instance = this._getInstance(buildCtx)
    const sqlBody = instance.getSqlBody(flag)
    return {
      template: instance.template,
      source: buildSqlBodyStr(buildCtx.adapter, sqlBody),
    }
  }



};