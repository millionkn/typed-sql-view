export const exec = <T>(fun: () => T): T => fun()

export function hasOneOf<T>(items: Iterable<T>, arr: NoInfer<T>[]) {
	return !![...items].find((e) => arr.includes(e))
}

export function pickConfig<K extends string, R>(key: K, config: { [key in K]: () => R }): R {
	return config[key]()
}
export type Async<T> = T | PromiseLike<T>

export function connectWith<T, V>(arr: readonly T[], getValue: (index: number) => V) {
	const result: (T | V)[] = []
	arr.forEach((t, i) => {
		if (i !== 0) {
			result.push(getValue(i))
		}
		result.push(t)
	})
	return result.flat()
}
