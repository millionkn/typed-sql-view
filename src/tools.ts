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

export function withPromiseResolvers<T>() {
	let resolve: (value: T) => void
	let reject: (reason?: any) => void
	const promise = new Promise<T>((res, rej) => {
		resolve = res
		reject = rej
	})
	return {
		promise,
		resolve: resolve!,
		reject: reject!,
	}
}

export type DeepTemplate<I> = I | (readonly [...DeepTemplate<I>[]]) | { readonly [key: string]: DeepTemplate<I> }

export const iterateTemplate = <I, O>(template: DeepTemplate<I>, isField: (value: unknown) => value is I, cb: (field: I) => O): DeepTemplate<O> => {
	if (isField(template)) {
		return cb(template)
	} else if (template instanceof Array) {
		return template.map((t) => iterateTemplate(t, isField, cb))
	} else {
		return Object.fromEntries(Object.entries(template).map(([key, t]) => [key, iterateTemplate(t, isField, cb)]))
	}
}