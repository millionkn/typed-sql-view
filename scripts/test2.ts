import { Adapter, SqlExecutor, createFromDefine } from '../src/index.js'
import z from 'zod'

class ElectAccidentEntity {
  static sqlView() {
    return createFromDefine(`"elect_accident_entity"`, (c) => {
      return {
        id: c((r) => `"${r}"."id"`).withNull(false).assert('', 'id').format((raw) => z.string().parse(raw)),
        所属大洲: c((r) => `"${r}"."所属大洲"`).withNull(false).assert('', 'string').format((raw) => z.string().parse(raw)),
        地点: c((r) => `"${r}"."地点"`).withNull(false).assert('', 'string').format((raw) => z.string().parse(raw)),
      }
    })
  }
}

const sql = ElectAccidentEntity.sqlView()
  .order('desc', (ref) => ref((e) => e.id))
  .forceMapTo((e, c) => {
    return {
      ...e,
      地点: c((ref) => `regexp_split_to_table(${ref(e.地点)}, '、')`).withNull(false).format((raw) => z.string().parse(raw)),
    }
  })
  .pipe((view) => Adapter.postgresAdapter.selectAll(view.mapTo((e) => {
    return {
      target: e.id.format((raw) => ({
        id: z.string().parse(raw),
      })),
      所属大洲: e.所属大洲,
      地点: e.地点,
    }
  })))
console.log(sql)