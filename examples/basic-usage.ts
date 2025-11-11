import { createSqlView, sql, createSqlAdapterForPostgres } from '@millionkn/typed-sql-view'
import type { SqlAdapter, SqlExecuteBundle } from '@millionkn/typed-sql-view'

// 基本用法示例 - 修正版本
// 展示如何创建表视图、执行查询和数据处理

// 1. 创建用户表视图
const userTable = createSqlView(sql`"users"`, (column) => {
	return {
		id: column((rootAlias) => sql`${rootAlias}."id"`)
			.withNull(false)
			.format((raw) => Number(raw)),

		name: column((rootAlias) => sql`${rootAlias}."name"`)
			.withNull(false)
			.format((raw) => String(raw)),

		email: column((rootAlias) => sql`${rootAlias}."email"`)
			.withNull(true)
			.format((raw) => raw ? String(raw) : null),

		createdAt: column((rootAlias) => sql`${rootAlias}."created_at"`)
			.withNull(false)
			.format((raw) => new Date(String(raw))),

		isActive: column((rootAlias) => sql`${rootAlias}."is_active"`)
			.withNull(false)
			.format((raw) => Boolean(raw)),
	}
})

// 2. 创建订单表视图
const orderTable = createSqlView(sql`"orders"`, (column) => {
	return {
		id: column((rootAlias) => sql`${rootAlias}."id"`)
			.withNull(false)
			.format((raw) => Number(raw)),

		userId: column((rootAlias) => sql`${rootAlias}."user_id"`)
			.withNull(false)
			.format((raw) => Number(raw)),

		amount: column((rootAlias) => sql`${rootAlias}."amount"`)
			.withNull(false)
			.format((raw) => Number(raw)),

		status: column((rootAlias) => sql`${rootAlias}."status"`)
			.withNull(false)
			.format((raw) => String(raw)),

		createdAt: column((rootAlias) => sql`${rootAlias}."created_at"`)
			.withNull(false)
			.format((raw) => new Date(String(raw))),
	}
})

// 3. 基本查询示例
export const basicQueries = {
	// 查询所有活跃用户
	activeUsers: userTable
		.andWhere((u) => sql`${u.isActive} = ${true}`),

	// 查询特定用户
	userById: (userId: number) => userTable
		.andWhere((u) => sql`${u.id} = ${userId}`),

	// 查询包含特定关键词的用户
	usersByName: (namePattern: string) => userTable
		.andWhere((u) => sql`${u.name} like ${`%${namePattern}%`}`),

	// 查询最近创建的用户
	recentUsers: (days: number) => userTable
		.andWhere((u) => sql`${u.createdAt} > ${new Date(Date.now() - days * 24 * 60 * 60 * 1000)}`)
		.order('DESC', 'LAST', (u) => u.createdAt),
}

// 4. 关联查询示例
export const joinQueries = {
	// 用户及其订单
	usersWithOrders: userTable
		.join('left join', orderTable)
		.on(({ base, extra }) => sql`${base.id} = ${extra.userId}`),

	// 有订单的用户
	usersWithActiveOrders: userTable
		.join('inner join', orderTable)
		.on(({ base, extra }) => sql`${base.id} = ${extra.userId}`)
		.andWhere(({ extra }) => sql`${extra.status} = ${'completed'}`),

	// 用户订单统计 - 修正版本
	userOrderStats: userTable
		.join('left join', orderTable)
		.on(({ base, extra }) => sql`${base.id} = ${extra.userId}`)
		.groupBy(
			({ base }) => ({ userId: base.id, userName: base.name }),
			(createColumn) => ({
				orderCount: createColumn(() => sql`count(*)`).withNull(false).format(Number),
				// 在groupBy中，createColumn的参数是函数，接收template参数
				totalAmount: createColumn((template) => sql`sum(${template.extra.amount})`).withNull(true).format(Number),
				avgAmount: createColumn((template) => sql`avg(${template.extra.amount})`).withNull(true).format(Number),
			})
		),
}

// 5. 复杂查询示例
export const complexQueries = {
	// 使用子查询 - 修正版本
	usersWithRecentOrders: userTable
		.andWhere((u) => sql`exists (
      ${orderTable
				.andWhere((o) => sql`${o.userId} = ${u.id}`)
				.andWhere((o) => sql`${o.createdAt} > ${new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)}`)
				.mapTo(() => []) // 转换为 select 1
			}
    )`),

	// 横向关联查询 - 获取每个用户的最新订单
	usersWithLatestOrder: userTable
		.lateralJoin('left join lazy withNull', (u) =>
			orderTable
				.andWhere((o) => sql`${o.userId} = ${u.id}`)
				.order('DESC', 'LAST', (o) => o.createdAt)
				.take(1)
		)
		.on(({ base, extra }) => sql`${base.id} = ${extra.userId}`),

	// 多条件过滤 - 修正版本
	filteredUsers: (filters: {
		name?: string
		email?: string
		activeOnly?: boolean
		minOrders?: number
	}) => {
		let query = userTable

		if (filters.name) {
			query = query.andWhere((u) => sql`${u.name} like ${`%${filters.name}%`}`)
		}

		if (filters.email) {
			query = query.andWhere((u) => sql`${u.email} like ${`%${filters.email}%`}`)
		}

		if (filters.activeOnly) {
			query = query.andWhere((u) => sql`${u.isActive} = ${true}`)
		}

		if (filters.minOrders !== undefined) {
			const minOrders = filters.minOrders
			query = query.andWhere((u) => sql`(
        ${orderTable
					.andWhere((o) => sql`${o.userId} = ${u.id}`)
					.groupBy(() => ({}), (createColumn) => ({
						count: createColumn(() => sql`count(*)`).withNull(false).format(Number)
					}))
					.mapTo((result) => result.aggrateValues.count)
				}
      ) >= ${minOrders}`)
		}

		return query
	},
}

// 6. 数据转换示例
export const transformationQueries = {
	// 转换用户数据格式
	userSummary: userTable
		.mapTo((u, { createColumn }) => ({
			displayName: createColumn(sql`concat(${u.name}, ' <', ${u.email}, '>')`)
				.withNull(false)
				.format(String),

			userType: createColumn(sql`case 
        when ${u.createdAt} > ${new Date(Date.now() - 30 * 24 * 60 * 60 * 1000)} then 'new'
        when ${u.createdAt} > ${new Date(Date.now() - 365 * 24 * 60 * 60 * 1000)} then 'regular'
        else 'old'
      end`)
				.withNull(false)
				.format(String),

			isVip: createColumn(sql`exists (
        ${orderTable
					.andWhere((o) => sql`${o.userId} = ${u.id}`)
					.andWhere((o) => sql`${o.amount} > ${1000}`)
					.mapTo(() => []) // 转换为 select 1
				}
      )`)
				.withNull(false)
				.format(Boolean),
		})),
}

// 7. 分页和排序示例
export const paginationQueries = {
	// 分页用户列表
	paginatedUsers: (page: number, pageSize: number) => userTable
		.order('DESC', 'LAST', (u) => u.createdAt)
		.skip((page - 1) * pageSize)
		.take(pageSize),

	// 多字段排序
	sortedUsers: userTable
		.order('DESC', 'LAST', (u) => u.createdAt)
		.order('ASC', 'FIRST', (u) => u.name),
}

// 8. 使用示例
export async function runBasicExamples() {
	const adapter = createSqlAdapterForPostgres()

	const sqlQuery = async <R>(getBundle: (adapter: SqlAdapter) => SqlExecuteBundle<R>) => {
		const bundle = getBundle(adapter)
		console.log('模拟执行', bundle.sql, bundle.paramArr)
		const results = [] as { [key: string]: unknown }[]
		return await bundle.formatter(results)
	}
	try {
		// 查询活跃用户
		const activeUsers = await sqlQuery((adapter) => adapter.selectAll(basicQueries.activeUsers))
		console.log('活跃用户:', activeUsers)

		// 查询用户统计
		const userStats = await sqlQuery((adapter) => adapter.selectAll(joinQueries.userOrderStats))
		console.log('用户统计:', userStats)

		// 查询单个用户
		const user = await sqlQuery((adapter) => adapter.selectOne(basicQueries.userById(1)))
		console.log('用户详情:', user)

		// 查询总数
		const totalUsers = await sqlQuery((adapter) => adapter.selectTotal(userTable))
		console.log('用户总数:', totalUsers)

		// 获取SQL和参数（用于调试）
		const queryBundle = adapter.selectAll(basicQueries.activeUsers)
		console.log('生成的SQL:', queryBundle.sql)
		console.log('参数数组:', queryBundle.paramArr)

	} catch (error) {
		console.error('查询执行错误:', error)
	}
}
