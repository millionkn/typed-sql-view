import { Column, ColumnDeclareFun, SqlViewTemplate } from "./define.js"
import { sym } from "./private.js"
import { SelectBody } from "./selectBody.js"
import { SqlView } from "./sqlView.js"


export function createFromDefine<const VT extends SqlViewTemplate>(
  rawFrom: string,
  getTemplate: (define: ColumnDeclareFun<string>) => VT
) {
  return new SqlView((ctx) => {
    const alias = ctx.genAlias()
    const template = getTemplate((getExpr) => Column[sym](getExpr(alias)))

    return {
      template,
      getStatement: (state) => {
        return new SelectBody({
          from: {
            alias,
            segment: rawFrom,
          },
          join: [],
          where: [],
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