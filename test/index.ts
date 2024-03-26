import { createFromDefine } from '../src/index.js'
import z from 'zod'

const tableDefine = createFromDefine(`"public"."tableName"`, (define) => {
  return {
    columnA: define(true, (root) => `"${root}"."column_a"`, (raw) => z.string().transform((v) => new Date(v)).parse(raw)),
    columnB: define(false, (root) => `"${root}"."column_b"`, (raw) => z.number().parse(raw)),
    columnC: define(true, (root) => `"${root}"."column_c"`),
    columnD: define(false, (root) => `"${root}"."column_d"`),
  }
})

const view = tableDefine
  .andWhere(({ ref, param }) => `${ref((e) => e.columnA)} = ${param('param')}`)
  .pipe((view) => {
    return view
      .groupBy((e) => [e.columnA], (define) => {
        return {
          maxB: define(false, (ref) => `max(${ref((e) => e.columnB)})`, (raw) => Number(raw)),
        }
      })
      .pipe((grouped) => {
        return view
          .join('lazy', true, grouped, (ctx) => `${ctx.ref((e) => e.base.columnA)} = ${ctx.ref((e) => e.extra.keys[0])}`)
      })
  }).mapTo((e) => {
    return e.extra
  })
  .buildSelect()
// const [] = rawSelect({ view, order: true, adapter }, (select) => {
//   return {

//   }
// })




