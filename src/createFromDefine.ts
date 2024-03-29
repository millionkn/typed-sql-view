import { SqlBody } from "./sqlBody.js";
import { SqlView } from "./sqlView.js";
import { Column, ColumnDeclareFun, InnerColumn, SqlViewTemplate } from "./tools.js";

export function createFromDefine<const VT extends SqlViewTemplate>(
  rawFrom: string,
  getTemplate: (define: ColumnDeclareFun<string>) => VT
) {
  return new SqlView(() => {
    const sym = {}
    const template = getTemplate((columnExpr) => new Column(new InnerColumn((ctx) => columnExpr(ctx.resolveSym(sym)))))
    return {
      template,
      analysis: (ctx) => {
        return new SqlBody({
          from: {
            aliasSym: sym,
            resolvable: () => rawFrom,
          },
          join: [],
          where: [],
          groupBy: [],
          having: [],
          order: [],
          take: null,
          skip: 0,
        })
      },
    }
  })
}