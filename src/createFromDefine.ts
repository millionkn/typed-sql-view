import { SqlBody } from "./sqlBody.js";
import { SqlView } from "./sqlView.js";
import { Column, ColumnDeclareFun, SqlViewTemplate } from "./tools.js";

export function createFromDefine<VT extends SqlViewTemplate>(
  rawFrom: string,
  getTemplate: (define: ColumnDeclareFun<string>) => VT
) {
  return new SqlView(() => {
    const info = new Map<Column, { columnExpr: (root: string) => string }>()
    const template = getTemplate((withNull, columnExpr, format = () => { throw new Error() }) => {
      const column = new Column<any, any>({ withNull, format })
      info.set(column, { columnExpr })
      return column
    })
    return {
      template,
      analysis: (ctx) => {
        const sym = {}
        ctx.usedColumn.forEach((column) => Column.setResolvable(column, ({ resolveSym }) => info.get(column)!.columnExpr(resolveSym(sym))))
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