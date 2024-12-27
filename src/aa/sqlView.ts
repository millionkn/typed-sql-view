import { Inner, BuildCtx, Column, Relation, SqlViewTemplate, flatViewTemplate, resolveSqlStr, GetColumnHolder, exec, sym, hasOneOf } from "./define.js"
import { SelectBody } from "./selectBody.js"

export type SelectState = {
  usedColumn: Column<boolean, unknown, string>[],
  order: boolean,
}

export type SelectRuntime<VT extends SqlViewTemplate<string>> = {
  template: VT,
  getSelectBody: (state: SelectState) => SelectBody,
}

export type ColumnDeclareFun<T> = (columnExpr: (input: T) => string) => Column

class ColumnHelper {
  public saved = new Map<Inner, Inner[]>()
  createColumn = (getExpr: (holder: (c: Inner) => string) => string) => {
    const expr = resolveSqlStr<Inner>((holder) => getExpr((inner) => holder(inner)))
    const inner: Inner = { expr: expr.map((e) => typeof e === 'string' ? e : e.expr).join('') }
    this.saved.set(inner, [...new Set(expr.flatMap((e) => typeof e === 'string' ? [] : this.saved.get(e) ?? e))])
    return Column[sym](inner)
  }
}

export class SqlView<VT1 extends SqlViewTemplate<string>> {
  constructor(
    public getInstance: (buildCtx: BuildCtx) => SelectRuntime<VT1>,
  ) { }

  pipe<R>(op: (self: this) => R): R {
    return op(this)
  }

  andWhere(getCondationStr: (tools: {
    ref: GetColumnHolder<VT1>,
    param: (value: any) => string,
    ctx: BuildCtx,
  }) => null | false | undefined | string): SqlView<VT1> {
    return new SqlView((ctx) => {
      const instance = this.getInstance(ctx)
      const expr = resolveSqlStr<Column>((holder) => {
        const str = getCondationStr({
          ref: (ref) => holder(ref(instance.template)),
          param: ctx.setParam,
          ctx,
        })
        return typeof str !== 'string' ? '' : str.trim()
      })
      return {
        template: instance.template,
        getSelectBody: (info) => {
          const usedColumn = info.usedColumn.concat(
            expr.filter((e): e is Column => typeof e !== 'string')
          )
          let sqlBody = instance.getSelectBody({
            order: info.order,
            usedColumn,
          })
          if (hasOneOf(sqlBody.state(), ['take', 'skip'])) {
            sqlBody = sqlBody.bracket(usedColumn)
          }
          const target = sqlBody.opts.groupBy.length === 0 ? sqlBody.opts.where : sqlBody.opts.having
          target.push(expr.flatMap((e) => {
            if (typeof e === 'string') { return e }
            return e[sym](true).inner.expr
          }).join(''))
          return sqlBody
        },
      }
    })
  }

  groupBy<const K extends SqlViewTemplate<''>, const VT extends SqlViewTemplate<''>>(
    getKeyTemplate: (vt: VT1) => K,
    getValueTemplate: (
      define: ColumnDeclareFun<GetColumnHolder<VT1>>,
    ) => VT,
  ): SqlView<{ keys: K, content: VT }> {
    return new SqlView((init) => {
      const columnHelper = new ColumnHelper()
      const instance = this.getInstance(init)
      const keys = getKeyTemplate(instance.template)
      const content = getValueTemplate((expr) => columnHelper.createColumn(
        (holder) => expr((ref) => holder(ref(instance.template)[sym](true).inner))
      ))
      return {
        template: { keys, content },
        getSelectBody: (info) => {
          const groupBy = [...new Set(flatViewTemplate(keys).map((c) => c[sym](true).inner))]
          const usedInner = info.usedColumn.map((c)=>c[sym](true).inner).flatMap((inner) => columnHelper.saved.get(inner) ?? inner).concat(groupBy)
          let sqlBody = instance.getSelectBody({
            order: info.order,
            usedColumn: info.usedColumn.concat(usedInner.map((inner)=>Column[sym](inner))) ,
          })
          if (hasOneOf(sqlBody.state(), ['order', 'groupBy', 'having', 'skip', 'take'])) {
            sqlBody = sqlBody.bracket(usedInner)
          }
          sqlBody.opts.groupBy = groupBy.map((inner) => () => inner.getStr())
          return sqlBody
        }
      }
    })
  }

  join<
    M extends 'left' | 'inner' | 'lazy',
    N extends { inner: false, left: boolean, lazy: boolean }[M],
    VT2 extends SqlViewTemplate,
  >(
    mode: M,
    withNull: N,
    view: SqlView<VT2>,
    getCondationExpr: (tools: {
      ref: GetColumnHolder<{ base: VT1, extra: VT2 }>,
    }) => string,
  ): SqlView<{ base: VT1, extra: Relation<N, VT2> }> {
    return new SqlView((init) => {
      const sp = exec(() => {
        const base = this.getInstance(init)
        const extra = view.getInstance(init)
        const condationExpr = resolveSqlStr<Inner>((holder) => getCondationExpr({
          ref: (ref) => holder(ref({
            base: base.template,
            extra: extra.template,
          }).opts.inner),
        }))
        const usedInnerInCondation = condationExpr
          .filter((e): e is Inner => typeof e === 'object')
        const extraColumnArr = flatViewTemplate(extra.template)
        extraColumnArr.forEach((c) => c.opts.withNull ||= withNull)
        return {
          base: {
            instance: base,
            inner: new Set(flatViewTemplate(base.template).map((e) => e.opts.inner)),
          },
          extra: {
            instance: extra,
            inner: new Set(flatViewTemplate(extra.template).map((e) => e.opts.inner)),
          },
          condationExpr,
          usedInnerInCondation,
        }
      })

      return {
        template: {
          base: sp.base.instance.template,
          extra: sp.extra.instance.template as Relation<N, VT2>,
        },
        getSqlBody: (info) => {
          if (mode === 'lazy') {
            if (!info.usedInner.find((inner) => sp.extra.inner.has(inner))) {
              return sp.base.instance.getSqlBody(info)
            }
            if (!withNull && !info.order) {
              if (!info.usedInner.find((inner) => sp.base.inner.has(inner))) {
                return sp.extra.instance.getSqlBody(info)
              }
            }
          }
          const usedInner = [...new Set(info.usedInner.concat(sp.usedInnerInCondation))]
          const baseUsedInner = usedInner.filter((inner) => sp.base.inner.has(inner))
          const extraUsedInner = usedInner.filter((inner) => sp.extra.inner.has(inner))
          const baseBody = exec(() => {
            let sqlBody = sp.base.instance.getSqlBody({
              order: info.order,
              usedInner: baseUsedInner,
            })
            if (hasOneOf(sqlBody.state(), ['groupBy', 'having', 'order', 'skip', 'take'])) {
              sqlBody = sqlBody.bracket(baseUsedInner)
            }
            return sqlBody
          })
          let extraBody = sp.extra.instance.getSqlBody({
            usedInner: extraUsedInner,
            order: pickConfig(mode satisfies 'inner' | 'left' | 'lazy', {
              'lazy': () => false,
              'left': () => info.order,
              'inner': () => info.order,
            }),
          })
          pickConfig(mode satisfies 'inner' | 'left' | 'lazy', {
            lazy: () => hasOneOf(extraBody.state(), ['innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
            left: () => hasOneOf(extraBody.state(), ['innerJoin', 'where', 'groupBy', 'having', 'order', 'skip', 'take']),
            inner: () => hasOneOf(extraBody.state(), ['leftJoin', 'innerJoin', 'groupBy', 'having', 'order', 'skip', 'take']),
          }) && (extraBody = extraBody.bracket(extraUsedInner))
          return new SqlBody(init, {
            from: baseBody.opts.from,
            join: [
              ...baseBody.opts.join ?? [],
              {
                type: pickConfig(mode satisfies 'inner' | 'left' | 'lazy', {
                  left: () => 'left',
                  inner: () => 'inner',
                  lazy: () => 'left',
                }),
                aliasSym: extraBody.opts.from.aliasSym,
                segment: extraBody.opts.from.segment,
                getCondation: () => sp.condationExpr.flatMap((e) => typeof e === 'string' ? e : e.getStr()).join(''),
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

  mapTo<const VT extends SqlViewTemplate>(getTemplate: (e: VT1, define: ColumnDeclareFun<(c: Column) => string>) => VT): SqlView<VT> {
    return new SqlView<VT>((init) => {
      const instance = this.getInstance(init)
      const columnHelper = new ColumnHelper()
      return {
        template: getTemplate(instance.template, (cr) => columnHelper.createColumn((ir) => cr((c) => ir(c.opts.inner)))),
        getSqlBody: (info) => {
          return instance.getSelectBody({
            order: info.order,
            usedColumn: [...new Set(info.usedInner.flatMap((inner) => columnHelper.saved.get(inner) ?? inner))]
          })
        },
      }
    })
  }

  bracketIf(condation: (opts: {
    state: SqlState[],
  }) => boolean) {
    return new SqlView<VT1>((init) => {
      const instance = this.getInstance(init)
      return {
        s
        template: instance.template,
        getSqlBody: (info) => {
          const sqlBody = instance.getSelectBody(info)
          if (condation({ state: [...sqlBody.state()] })) {
            return sqlBody.bracket(info.usedInner)
          } else {
            return sqlBody
          }
        },
      }
    })
  }

  order(
    order: 'asc' | 'desc',
    getExpr: (ref: GetColumnHolder<VT1>) => false | null | undefined | string,
  ): SqlView<VT1> {
    return new SqlView((init) => {
      const instance = this.getInstance(init)
      return {
        template: instance.template,
        getSqlBody: (info) => {
          if (!info.order) { return instance.getSelectBody(info) }
          const expr = resolveSqlStr<Inner>((holder) => {
            const expr = getExpr((ref) => holder(ref(instance.template).opts.inner))
            if (typeof expr === 'string') { return expr.trim() }
            return ''
          })
          const usedInner = [...new Set(info.usedInner.concat(expr.filter((e): e is Inner => typeof e === 'object')))]
          let sqlBody = instance.getSelectBody({
            order: info.order,
            usedColumn: usedInner,
          })
          if (hasOneOf(sqlBody.state(), ['skip', 'take'])) {
            sqlBody = sqlBody.bracket(usedInner)
          }
          sqlBody.opts.order.unshift({
            order,
            getStr: () => expr.flatMap((e) => typeof e === 'string' ? e : e.getStr()).join(''),
          })
          return sqlBody
        },
      }
    })
  }

  take(count: number | null | undefined | false): SqlView<VT1> {
    if (typeof count !== 'number') { return this }
    return new SqlView((init) => {
      const instance = this.getInstance(init)
      return {
        template: instance.template,
        getSqlBody: (ctx) => {
          const sqlBody = instance.getSelectBody({
            order: true,
            usedColumn: ctx.usedInner,
          })
          sqlBody.opts.take = sqlBody.opts.take === null ? count : Math.min(sqlBody.opts.take, count)
          return sqlBody
        },
      }
    })
  }

  skip(count: number | null | undefined | false): SqlView<VT1> {
    if (typeof count !== 'number' || count <= 0) { return this }
    return new SqlView((init) => {
      const instance = this
        .bracketIf(({ state }) => hasOneOf(state, ['take']))
        .getInstance(init)
      return {
        template: instance.template,
        getSqlBody: (ctx) => {
          const sqlBody = instance.getSelectBody({
            order: true,
            usedColumn: ctx.usedInner,
          })
          sqlBody.opts.skip = sqlBody.opts.skip + count
          sqlBody.opts.take = sqlBody.opts.take === null ? null : Math.max(0, sqlBody.opts.take - count)
          return sqlBody
        },
      }
    })
  }
};