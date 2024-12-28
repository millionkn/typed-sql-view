import { sym, Column, SqlViewTemplate, SqlBody } from "./tools.js"
import { SqlView } from "./sqlView.js"

export function buildSqlView<const VT extends SqlViewTemplate<string>>(
  getTemplate: (column: (expr: string) => Column<''>, opts: {
    addFrom: (raw: string) => string,
    leftJoin: (raw: string, condation: (alias: string) => string) => string,
    innerJoin: (raw: string, condation: (alias: string) => string) => string,
    andWhere: (condation: string) => void,
  }) => VT
) {
  return new SqlView((ctx) => {
    const sqlBody: SqlBody = {
      from: [],
      join: [],
      where: [],
      groupBy: [],
      having: [],
      order: [],
      take: null,
      skip: 0,
    }
    const template = getTemplate((expr) => {
      return Column[sym]({
        expr,
        declareUsed: () => { },
      })
    }, {
      addFrom: (raw) => {
        const alias = ctx.genAlias()
        sqlBody.from.push({
          alias,
          expr: raw,
        })
        return alias
      },
      andWhere: (c) => sqlBody.where.push(c),
      innerJoin: (raw, condation) => {
        const alias = ctx.genAlias()
        sqlBody.join.push({
          type: 'inner',
          alias,
          expr: raw,
          condation: condation(alias),
        })
        return alias
      },
      leftJoin: (raw, condation) => {
        const alias = ctx.genAlias()
        sqlBody.join.push({
          type: 'left',
          alias,
          expr: raw,
          condation: condation(alias),
        })
        return alias
      },
    })

    return {
      template,
      getSqlBody: () => sqlBody
    }
  })
}