import { SqlBody } from "./sqlBody.js"
import { SqlState, Column } from "./tools.js"


export function bracketIf(opts: {
  usedColumn: Column[],
  sqlBody: SqlBody,
  useWrap: (opts: {
    state: SqlState[]
  }) => boolean
}) {
  if (!opts.useWrap({ state: opts.sqlBody.state() })) { return opts.sqlBody }
  const aliasSym = {}
  const mapper = new Map([...new Set(opts.usedColumn)].map((column, index) => {
    const resolvable = Column.getResolvable(column)
    const alias = `value_${index}`
    Column.setResolvable(column, ({ resolveSym }) => `"${resolveSym(aliasSym)}"."${alias}"`);
    return [resolvable, alias]
  }))
  return new SqlBody({
    from: {
      aliasSym,
      resolvable: (ctx) => `(${opts.sqlBody.build(mapper, ctx)})`,
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

