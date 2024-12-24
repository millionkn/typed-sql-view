import { sym, Column, ColumnDeclareFun, SqlViewTemplate } from "./define.js"
import { SelectBody } from "./selectBody.js"
import { SqlView } from "./sqlView.js"


export function createFromDefine<const VT extends SqlViewTemplate>(
  rawFrom: string,
  getTemplate: (
    define: ColumnDeclareFun<string>,
    tools: {
      andWhere: (getExpr: (alias: string, param: (value: unknown) => string) => string) => void,
    }
  ) => VT
) {
  return new SqlView((ctx) => {
    const alias = ctx.getAlias()
    const andWhere: string[] = []
    const template = getTemplate(
      (getExpr) => Column[sym](getExpr(alias)),
      {
        andWhere: (getExpr) => {
          andWhere.push(getExpr(alias, (value) => ctx.setParam(value)))
        },
      }
    )

    return {
      template,
      getSelectBody: () => {
        return new SelectBody(ctx, {
          from: {
            alias,
            segment: rawFrom,
          },
          join: [],
          where: andWhere,
          groupBy: [],
          having: [],
          order: [],
          take: null,
          skip: 0,
        })
      }
    }
  })
}