import { createSqlView } from "./createSqlView.js"
import { Column, createColumn } from "./tools.js"

export function resolveTableDefine<const T extends {
	[key: string]: {
		rawKey: string,
		schema: (raw: unknown) => unknown
	} & Record<string, unknown>
}>(template: T) {
	const columnArr = Object.entries(template).map(([key, meta]) => {
		return { key, meta }
	}) as { [key in keyof T]: { key: key, meta: T[key] } }[keyof T][]
	return {
		columnArr,
		klass: class { } as { new(): { [key in keyof T]: ReturnType<T[key]['schema']> } },
		getSqlView: (from: string) => createSqlView(({ addFrom }) => {
			const alias = addFrom(from)
			return Object.fromEntries(columnArr.map(({ key, meta }) => {
				return [key, createColumn(`${alias}.${meta.rawKey}`).withNull(false).format((raw) => meta.schema(raw))]
			})) as { [key in keyof T]: Column<'', false, ReturnType<T[key]['schema']>> }
		}),
	}
}