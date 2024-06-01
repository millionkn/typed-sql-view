export const privateSym = Symbol()

export const exec = <T>(fun: () => T): T => fun()

export function hasOneOf<T>(items: Iterable<T>, arr: (T & {})[]) {
  return !![...items].find((e) => arr.includes(e as any))
}

export type DeepTemplate<I> = I | (readonly DeepTemplate<I>[]) | { readonly [key: string]: DeepTemplate<I> }

export function pickConfig<K extends string | number, R>(key: K, config: { [key in K]: () => R }): R {
  return config[key]()
}