export const sym = Symbol()

export const exec = <T>(fun: () => T): T => fun()
export const skipHolder = `'"\`skip'"\``
export const takeHolder = `'"\`take'"\``
export type DeepTemplate<I> = I | (readonly DeepTemplate<I>[]) | { readonly [key: string]: DeepTemplate<I> }

export type Inner = {
  expr: string
}