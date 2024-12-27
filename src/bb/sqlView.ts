import { Adapter, BuildCtx, SqlViewTemplate } from "./define.js"
import { SqlBody } from "./sqlBody.js"

export type SelectState = {
  order: boolean,
}

export type SelectRuntime<VT extends SqlViewTemplate<string>> = {
  template: VT,
  getSqlBody: (state: SelectState) => SqlBody,
}


export class SqlView<VT1 extends SqlViewTemplate<string>> {
  constructor(
    public getInstance: (buildCtx: BuildCtx, adapter: Adapter) => SelectRuntime<VT1>,
  ) { }

  pipe<R>(op: (self: this) => R): R {
    return op(this)
  }
};