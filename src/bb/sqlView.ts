import { BuildCtx, GetColumnRef, Inner, Relation, SqlBody, SqlViewTemplate, buildSqlBodyStr, exec, flatViewTemplate, hasOneOf, pickConfig, sqlBodyState, sym } from "./tools.js"

export type BuildFlag = {
  order: boolean,
}

export type SelectRuntime<VT extends SqlViewTemplate<string>> = {
  template: VT,
  getSqlBody: (flag: BuildFlag) => SqlBody,
}


export class SqlView<VT1 extends SqlViewTemplate<string>> {
  constructor(
    private _getInstance: (buildCtx: BuildCtx) => SelectRuntime<VT1>,
  ) { }

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

      let hasFromBase = false
      let hasFromExtra = false
      const sp = exec(() => {
        const base = this._getInstance(ctx)
        const extra = view._getInstance(ctx)
        const condationExpr = exec(() => {
          const resolver = ctx.createResolver<Inner>()
          return resolver.resolve(getCondationExpr((ref) => resolver.createHolder(() => ref({
            base: base.template,
            extra: extra.template,
          })[sym].inner)))
        })

        flatViewTemplate(base.template).forEach((c) => {
          const inner = c[sym].inner
          inner.declareUsed = () => {
            hasFromBase = true
            
          }
        })
        const allExtra = flatViewTemplate(extra.template)
        if (withNull) {
          allExtra.forEach((c) => c[sym].withNull = true)
        }
        allExtra.forEach((c) => {
          const inner = c[sym].inner
          inner.declareUsed = () => {
            hasFromExtra = true
            return { ref: resolver.createHolder(() => inner) }
          }
        })

        return {
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
        getSqlBody: (info) => {
          if (mode === 'lazy') {
            if (!hasFromExtra) {
              return sp.base.getSqlBody(info)
            }
            if (!withNull && !info.order) {
              if (!hasFromBase) {
                return sp.extra.getSqlBody(info)
              }
            }
          }
          const condation = sp.condationExpr.map((e) => typeof e === 'string' ? e : e().declareUsed().ref).join('')
          const baseBody = exec(() => {
            let sqlBody = sp.base.getSqlBody({
              order: info.order,
            })
            const bracket = hasOneOf(sqlBodyState(sqlBody), ['groupBy', 'having', 'order', 'skip', 'take'])
            if (bracket) { sqlBody = sqlBody.bracket() }
            return sqlBody
          })
          let extraBody = exec(() => {
            let extraBody = sp.extra.getSqlBody({
              order: pickConfig(mode, {
                'lazy': () => false,
                'left': () => info.order,
                'inner': () => info.order,
              }),
            })
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