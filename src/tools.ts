import { Column, Inner, SqlViewTemplate } from "./define.js"
import { exec, privateSym } from "./private.js"

export const resolveSqlStr = exec(() => {
  let _nsIndex = 0
  return <V extends object>(getExpr: (holder: (value: V) => string) => string) => {
    const nsIndex = _nsIndex += 1
    const split = `'"\`${nsIndex}'"\``
    let index = 0
    const saved = new Map<string, V>()
    const expr = getExpr((value) => {
      const key = `holder_${nsIndex}_${index += 1}`
      saved.set(key, value)
      return `${split}${key}${split}`
    })
    _nsIndex -= 1
    return expr.length === 0 ? [] : expr.split(split).map((str, i) => {
      if (i % 2 === 0) { return str }
      const r = saved.get(str)
      if (!r) { throw new Error() }
      return r
    })
  }
})

function _flatViewTemplate(template: SqlViewTemplate): Column[] {
  if (template instanceof Column) { return [template] }
  if (template instanceof Array) { return template.flatMap((e) => _flatViewTemplate(e)) }
  return Object.values(template).flatMap((e) => _flatViewTemplate(e))
}

export function flatViewTemplate(template: SqlViewTemplate) {
  return _flatViewTemplate(template)
}

export const createColumnHelper = () => {
  const saved = new Map<Inner, Inner[]>()
  return {
    getDeps: (inner: Inner) => saved.get(inner) ?? null,
    createColumn: (getExpr: (holder: (c: Inner) => string) => string) => {
      const expr = resolveSqlStr<Inner>((holder) => getExpr((inner) => holder(inner)))
      const inner: Inner = {
        [privateSym]: 'inner',
        segment: expr.flatMap((e) => typeof e === 'string' ? e : e.segment)
      }
      saved.set(inner, [...new Set(expr.flatMap((e) => typeof e === 'string' ? [] : saved.get(e) ?? e))])
      return Column[privateSym](inner)
    },
  }
}