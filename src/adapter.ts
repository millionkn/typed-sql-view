export class SqlAdapter {
  constructor(
    public opts: {
      paramHolder: (index: number) => string,
      skip: 'skip' | 'offset',
      take: 'take' | 'limit',
    }
  ) { }
}