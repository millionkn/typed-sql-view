import { SqlAdapter, createSqlView, sql, SqlExecuteBundle } from '@millionkn/typed-sql-view'

// 自定义适配器示例
// 展示如何为不同的数据库创建适配器

// 1. SQLite 适配器
export const createSqliteAdapter = () => {
	return new SqlAdapter({
		paramHolder: () => '?',
		adapter: {
			tableAlias: (alias) => `"${alias}"`,
			columnRef: (tableAlias, columnAlias) => `"${tableAlias}"."${columnAlias}"`,
			selectAndAlias: (select, alias) => `${select} as "${alias}"`,
			pagination: (skip, take) => {
				const parts: string[] = []
				if (take !== null) parts.push(`limit ${take}`)
				if (skip > 0) parts.push(`offset ${skip}`)
				return parts.join(' ')
			},
			order: (items) => `order by ${items.map(({ expr, order, nulls }) => {
				// SQLite 不支持 NULLS FIRST/LAST，使用 CASE 语句模拟
				if (nulls === 'FIRST') {
					return `case when ${expr} is null then 0 else 1 end, ${expr} ${order}`
				} else {
					return `case when ${expr} is null then 1 else 0 end, ${expr} ${order}`
				}
			}).join(', ')}`,
		}
	})
}

// 2. Oracle 适配器
export const createOracleAdapter = () => {
	return new SqlAdapter({
		paramHolder: (index) => `:param${index + 1}`,
		adapter: {
			tableAlias: (alias) => `"${alias}"`,
			columnRef: (tableAlias, columnAlias) => `"${tableAlias}"."${columnAlias}"`,
			selectAndAlias: (select, alias) => `${select} as "${alias}"`,
			pagination: (skip, take) => {
				// Oracle 使用 ROWNUM 或 ROW_NUMBER() 进行分页
				if (take !== null && skip > 0) {
					return `offset ${skip} rows fetch next ${take} rows only`
				} else if (take !== null) {
					return `fetch first ${take} rows only`
				} else if (skip > 0) {
					return `offset ${skip} rows`
				}
				return ''
			},
			order: (items) => `order by ${items.map(({ expr, order, nulls }) =>
				`${expr} ${order} nulls ${nulls}`
			).join(', ')}`,
		}
	})
}

// 3. SQL Server 适配器
export const createSqlServerAdapter = () => {
	return new SqlAdapter({
		paramHolder: (index) => `@param${index + 1}`,
		adapter: {
			tableAlias: (alias) => `[${alias}]`,
			columnRef: (tableAlias, columnAlias) => `[${tableAlias}].[${columnAlias}]`,
			selectAndAlias: (select, alias) => `${select} as [${alias}]`,
			pagination: (skip, take) => {
				// SQL Server 使用 OFFSET/FETCH
				const parts: string[] = []
				if (skip > 0) parts.push(`offset ${skip} rows`)
				if (take !== null) parts.push(`fetch next ${take} rows only`)
				return parts.join(' ')
			},
			order: (items) => `order by ${items.map(({ expr, order, nulls }) => {
				// SQL Server 的 NULL 排序语法
				if (nulls === 'FIRST') {
					return `${expr} ${order} nulls first`
				} else {
					return `${expr} ${order} nulls last`
				}
			}).join(', ')}`,
		}
	})
}

// 4. 自定义语法适配器（支持特殊需求）
export const createCustomAdapter = (options: {
	caseSensitive: boolean
	quoteChar: string
	parameterStyle: 'question' | 'dollar' | 'at' | 'colon'
	supportsNullsFirst: boolean
}) => {
	const { caseSensitive, quoteChar, parameterStyle, supportsNullsFirst } = options

	const quote = (str: string) => `${quoteChar}${str}${quoteChar}`

	const getParamHolder = (): ((index: number) => string) => {
		switch (parameterStyle) {
			case 'question': return () => '?'
			case 'dollar': return (index: number) => `$${index + 1}`
			case 'at': return (index: number) => `@param${index + 1}`
			case 'colon': return (index: number) => `:param${index + 1}`
			default: return () => '?'
		}
	}

	return new SqlAdapter({
		paramHolder: getParamHolder(),
		adapter: {
			tableAlias: (alias) => quote(alias),
			columnRef: (tableAlias, columnAlias) => `${quote(tableAlias)}.${quote(columnAlias)}`,
			selectAndAlias: (select, alias) => `${select} as ${quote(alias)}`,
			pagination: (skip, take) => {
				const parts: string[] = []
				if (take !== null) parts.push(`limit ${take}`)
				if (skip > 0) parts.push(`offset ${skip}`)
				return parts.join(' ')
			},
			order: (items) => {
				const orderClause = items.map(({ expr, order, nulls }) => {
					if (supportsNullsFirst) {
						return `${expr} ${order} nulls ${nulls}`
					} else {
						// 使用 CASE 语句模拟 NULL 排序
						if (nulls === 'FIRST') {
							return `case when ${expr} is null then 0 else 1 end, ${expr} ${order}`
						} else {
							return `case when ${expr} is null then 1 else 0 end, ${expr} ${order}`
						}
					}
				}).join(', ')

				return `order by ${orderClause}`
			},
		}
	})
}

// 5. 扩展适配器（添加自定义功能）
export class ExtendedSqlAdapter extends SqlAdapter {
	constructor(
		private baseAdapter: SqlAdapter,
		private customOptions: {
			enableQueryCache?: boolean
			customFormatters?: Map<string, (value: any) => any>
			queryHooks?: {
				beforeQuery?: (sql: string, params: any[]) => void
				afterQuery?: (sql: string, params: any[], result: any) => void
			}
		}
	) {
		super({
			paramHolder: (index) => '?', // 占位符，实际使用 baseAdapter
			adapter: {
				tableAlias: () => '',
				columnRef: () => '',
				selectAndAlias: () => '',
				pagination: () => '',
				order: () => '',
			}
		})
	}

	// 重写 selectAll 方法添加缓存
	selectAll<VT>(view: any): SqlExecuteBundle<any[]> {
		const bundle = this.baseAdapter.selectAll(view)

		// 执行查询前的钩子
		this.customOptions.queryHooks?.beforeQuery?.(bundle.sql, bundle.paramArr)

		// 返回修改后的bundle
		return {
			...bundle,
			formatter: async (results: any[]): Promise<any[]> => {
				try {
					const formattedResults = await bundle.formatter(results)

					// 执行查询后的钩子
					this.customOptions.queryHooks?.afterQuery?.(bundle.sql, bundle.paramArr, formattedResults)

					return formattedResults
				} catch (error) {
					console.error('查询执行错误:', error)
					throw error
				}
			}
		}
	}

	// 添加自定义格式化器
	withCustomFormatter<T>(columnName: string, formatter: (value: T) => any) {
		this.customOptions.customFormatters?.set(columnName, formatter)
		return this
	}
}

// 6. 数据库特定的优化适配器
export const createOptimizedAdapter = (databaseType: 'mysql' | 'postgres' | 'sqlite') => {
	const baseAdapter = SqlAdapter.createPostgresAdapter() // 默认使用 PostgreSQL

	return {
		...baseAdapter,

		// MySQL 特定优化
		mysql: databaseType === 'mysql' ? {
			...SqlAdapter.createMySqlAdapter(),
			// 添加 MySQL 特定的优化
			selectAll: async <VT>(view: any) => {
				const bundle = SqlAdapter.createMySqlAdapter().selectAll(view)

				// MySQL 特定的查询优化
				const optimizedSql = bundle.sql
					.replace(/ilike/gi, 'like') // MySQL 不支持 ilike
					.replace(/nulls\s+(first|last)/gi, '') // MySQL 不支持 nulls first/last

				return {
					...bundle,
					sql: optimizedSql
				}
			}
		} : null,

		// SQLite 特定优化
		sqlite: databaseType === 'sqlite' ? {
			...createSqliteAdapter(),
			// 添加 SQLite 特定的优化
			selectAll: async <VT>(view: any) => {
				const bundle = createSqliteAdapter().selectAll(view)

				// SQLite 特定的查询优化
				const optimizedSql = bundle.sql
					.replace(/ilike/gi, 'like') // SQLite 不支持 ilike
					.replace(/nulls\s+(first|last)/gi, '') // SQLite 不支持 nulls first/last

				return {
					...bundle,
					sql: optimizedSql
				}
			}
		} : null,
	}
}

// 7. 使用示例
export async function runCustomAdapterExamples() {
	// 使用不同的适配器
	const adapters = {
		sqlite: createSqliteAdapter(),
		oracle: createOracleAdapter(),
		sqlServer: createSqlServerAdapter(),
		custom: createCustomAdapter({
			caseSensitive: true,
			quoteChar: '`',
			parameterStyle: 'question',
			supportsNullsFirst: false,
		}),
	}

	// 创建测试视图
	const testTable = createSqlView(sql`"test_table"`, (column) => ({
		id: column((root) => sql`${root}."id"`).withNull(false).format(Number),
		name: column((root) => sql`${root}."name"`).withNull(false).format(String),
	}))

	const testQuery = testTable
		.andWhere((t) => sql`${t.id} > ${100}`)
		.order('ASC', 'FIRST', (t) => t.name)
		.take(10)

	const sqlQuery = async <R>(adapter: SqlAdapter, getBundle: (adapter: SqlAdapter) => SqlExecuteBundle<R>) => {
		const bundle = getBundle(adapter)
		console.log('模拟执行', bundle.sql, bundle.paramArr)
		const results = [] as { [key: string]: unknown }[]
		return await bundle.formatter(results)
	}

	// 测试不同适配器生成的 SQL
	for (const [name, adapter] of Object.entries(adapters)) {
		try {
			const bundle = adapter.selectAll(testQuery)
			console.log(`${name} 适配器生成的 SQL:`, bundle.sql)
			console.log(`${name} 适配器参数:`, bundle.paramArr)
			console.log('---')
		} catch (error) {
			console.error(`${name} 适配器错误:`, error)
		}
	}

	// 使用扩展适配器
	const extendedAdapter = new ExtendedSqlAdapter(
		SqlAdapter.createPostgresAdapter(),
		{
			enableQueryCache: true,
			customFormatters: new Map([
				['name', (value: string) => value.toUpperCase()],
			]),
			queryHooks: {
				beforeQuery: (sql, params) => {
					console.log('执行查询前:', { sql, params })
				},
				afterQuery: (sql, params, result) => {
					console.log('执行查询后:', { sql, params, resultCount: result.length })
				},
			},
		}
	)

	// 使用扩展适配器
	try {
		const results = await sqlQuery(extendedAdapter, (adapter) => adapter.selectAll(testQuery))
		console.log('扩展适配器结果:', results)
	} catch (error) {
		console.error('扩展适配器错误:', error)
	}
}
