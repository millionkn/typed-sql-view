import { exec, skipHolder, takeHolder } from "./private.js"


export class SelectBody {
  constructor(public opts: {
    from: {
      alias: string,
      segment: string,
    },
    join: {
      type: 'left' | 'inner',
      alias: string,
      segment: string,
      condation: string,
    }[],
    where: string[],
    groupBy: string[],
    having: string[],
    order: {
      order: 'asc' | 'desc',
      segment: string,
    }[],
    take: null | number,
    skip: number,
  }) { }

  getSegment() {
    let buildResult: string[] = []
    exec(() => {
      const e = this.opts.from
      buildResult.push(`from ${e.segment} ${e.alias}`) 
    })
    this.opts.join.forEach((join) => {
      buildResult.push(`${join.type} join ${join.segment} ${join.alias} on ${join.condation}`)
    })
    exec(() => {
      if (this.opts.where.length === 0) { return } 
      buildResult.push(`where ${this.opts.where.join(' and ')}`)
    })
    exec(() => {
      if (this.opts.groupBy.length === 0) { return }
      buildResult.push(`group by ${this.opts.groupBy.join(',')}`) 
    })
    exec(() => {
      if (this.opts.having.length === 0) { return }
      buildResult.push(`having ${this.opts.having.join(' and ')}`) 
    })
    exec(() => {
      if (this.opts.order.length === 0) { return }
      buildResult.push(`order by ${this.opts.order.map((e)=>`${e.segment} ${e.order}`).join(',')}`)
    })
    if (this.opts.skip) {
      buildResult.push(skipHolder, this.opts.skip.toString())
    }
    if (this.opts.take !== null) {
      buildResult.push(takeHolder, this.opts.take.toString())
    }
    return buildResult.join(' ')
  }
}