import { AliasSym, Column, SqlViewTemplate } from "./define.js";
import { privateSym } from "./private.js";
import { SqlBody } from "./sqlBody.js";
import { ColumnDeclareFun, SqlView } from "./sqlView.js";

export function createFromDefine<const VT extends SqlViewTemplate>(
  rawFrom: string,
  getTemplate: (define: ColumnDeclareFun<string>) => VT
) {
  return new SqlView((init) => {
    const sym = new AliasSym()
    const template = getTemplate((columnExpr) => Column[privateSym]({
      [privateSym]: null,
      getStr: () => columnExpr(sym.getAlias()),
    }))
    return {
      template,
      getSqlBody: () => {
        return new SqlBody(init, {
          from: {
            aliasSym: sym,
            segment: [rawFrom],
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