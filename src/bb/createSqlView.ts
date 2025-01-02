import { SqlViewTemplate, SqlBody, Column, sym } from "./tools.js"
import { SqlView } from "./sqlView.js"

export function createSqlView<VT extends SqlViewTemplate<string>>(
  getTemplate: (column: (expr: string) => Column<''>, opts: {
    addFrom: (expr: string) => string,
    leftJoin: (expr: string, condation: (alias: string) => string) => string,
    innerJoin: (expr: string, condation: (alias: string) => string) => string,
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
    const template = getTemplate((expr) => Column[sym]({ expr }), {
      addFrom: (expr) => {
        const alias = ctx.genAlias()
        sqlBody.from.push({
          alias,
          expr,
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
      getSqlBody: ({ order }) => {
        if (!order) { sqlBody.order = [] }
        return sqlBody
      }
    }
  })
}