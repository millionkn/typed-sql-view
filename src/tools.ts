export const exec = <T>(fun: () => T): T => fun()

export function hasOneOf<T>(items: Iterable<T>, arr: NoInfer<T>[]) {
	return !![...items].find((e) => arr.includes(e))
}

export function pickConfig<K extends string, R>(key: K, config: { [key in K]: () => R }): R {
	return config[key]()
}
export type Async<T> = T | PromiseLike<T>