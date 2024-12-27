import { sym, Column, SqlViewTemplate } from "./define.js"
import { SqlBody } from "./sqlBody.js"
import { SqlView } from "./sqlView.js"

export function createFromDefine<const VT extends SqlViewTemplate<string>>(
  getTemplate: (opts: {
    addFrom: (raw: string) => string,
    leftJoin: (raw: string, condation: (alias: string) => string) => string,
    innerJoin: (raw: string, condation: (alias: string) => string) => string,
    andWhere: (condation: string) => void,
    column: (expr: string) => Column<''>,
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
    const template = getTemplate({
      addFrom: (raw) => {
        const alias = ctx.genAlias()
        sqlBody.from.push({
          alias,
          segment: raw,
        })
        return alias
      },
      andWhere: (c) => sqlBody.where.push(c),
      innerJoin: (raw, condation) => {
        const alias = ctx.genAlias()
        sqlBody.join.push({
          type: 'inner',
          alias,
          segment: raw,
          condation: condation(alias),
        })
        return alias
      },
      leftJoin: (raw, condation) => {
        const alias = ctx.genAlias()
        sqlBody.join.push({
          type: 'left',
          alias,
          segment: raw,
          condation: condation(alias),
        })
        return alias
      },
      column: (expr) => {
        return Column[sym]({
          declareUsed: () => {
            return {
              ref: expr,
            }
          },
        })
      },
    })

    return {
      template,
      getSqlBody: () => {
        return sqlBody
      }
    }
  })
}