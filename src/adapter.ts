export class SqlAdapter {
  private constructor(
    public opts: {
      paramHolder: (opts: { alias: string, value: unknown, index: number }) => [string, unknown],
      executor: (sql: string, params: unknown[]) => Promise<{ [key: string]: unknown }[]>,
      skip: 'skip' | 'offset',
      take: 'take' | 'limit',
    }
  ) { }

  static create<V>(
    paramHolder: (opts: { alias: string, value: unknown, index: number }) => [string, V],
    executor: (sql: string, params: V[]) => Promise<{ [key: string]: unknown }[]>,
    skip: 'skip' | 'offset',
    take: 'take' | 'limit',
  ) {
    return new SqlAdapter({
      paramHolder,
      executor: executor as any,
      take,
      skip,
    })
  }
}