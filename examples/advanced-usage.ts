import { createSqlView, sql, SqlAdapter, SqlExecuteBundle } from '@millionkn/typed-sql-view'

// 高级用法示例
// 展示复杂查询、性能优化和高级特性

// 1. 复杂表结构示例
const productTable = createSqlView(sql`"products"`, (column) => {
	return {
		id: column((rootAlias) => sql`${rootAlias}."id"`)
			.withNull(false)
			.format(Number),

		name: column((rootAlias) => sql`${rootAlias}."name"`)
			.withNull(false)
			.format(String),

		categoryId: column((rootAlias) => sql`${rootAlias}."category_id"`)
			.withNull(false)
			.format(Number),

		price: column((rootAlias) => sql`${rootAlias}."price"`)
			.withNull(false)
			.format(Number),

		stock: column((rootAlias) => sql`${rootAlias}."stock"`)
			.withNull(false)
			.format(Number),

		isActive: column((rootAlias) => sql`${rootAlias}."is_active"`)
			.withNull(false)
			.format(Boolean),
	}
})

const categoryTable = createSqlView(sql`"categories"`, (column) => {
	return {
		id: column((rootAlias) => sql`${rootAlias}."id"`)
			.withNull(false)
			.format(Number),

		name: column((rootAlias) => sql`${rootAlias}."name"`)
			.withNull(false)
			.format(String),

		parentId: column((rootAlias) => sql`${rootAlias}."parent_id"`)
			.withNull(true)
			.format((raw) => raw ? Number(raw) : null),
	}
})

const orderItemTable = createSqlView(sql`"order_items"`, (column) => {
	return {
		id: column((rootAlias) => sql`${rootAlias}."id"`)
			.withNull(false)
			.format(Number),

		orderId: column((rootAlias) => sql`${rootAlias}."order_id"`)
			.withNull(false)
			.format(Number),

		productId: column((rootAlias) => sql`${rootAlias}."product_id"`)
			.withNull(false)
			.format(Number),

		quantity: column((rootAlias) => sql`${rootAlias}."quantity"`)
			.withNull(false)
			.format(Number),

		unitPrice: column((rootAlias) => sql`${rootAlias}."unit_price"`)
			.withNull(false)
			.format(Number),
	}
})

// 2. 递归查询示例（层级分类）
export const hierarchicalQueries = {
	// 获取分类树（包含子分类）
	categoryTree: categoryTable
		.mapTo((c, { createColumn }) => ({
			...c,
			level: createColumn(sql`
        with recursive category_levels as (
          select id, name, parent_id, 0 as level
          from ${categoryTable}
          where parent_id is null
          
          union all
          
          select c.id, c.name, c.parent_id, cl.level + 1
          from ${categoryTable} c
          join category_levels cl on c.parent_id = cl.id
        )
        select level from category_levels where id = ${c.id}
      `).withNull(false).format(Number),

			path: createColumn(sql`
        with recursive category_paths as (
          select id, name, parent_id, name as path
          from ${categoryTable}
          where parent_id is null
          
          union all
          
          select c.id, c.name, c.parent_id, 
                 cp.path || ' > ' || c.name
          from ${categoryTable} c
          join category_paths cp on c.parent_id = cp.id
        )
        select path from category_paths where id = ${c.id}
      `).withNull(false).format(String),
		})),
}

// 3. 窗口函数示例
export const windowFunctionQueries = {
	// 产品销售排名
	productRankings: productTable
		.mapTo((p, { createColumn }) => ({
			...p,
			// 按价格排名
			priceRank: createColumn(sql`
        rank() over (order by ${p.price} desc)
      `).withNull(false).format(Number),

			// 按类别内的价格排名
			categoryPriceRank: createColumn(sql`
        rank() over (partition by ${p.categoryId} order by ${p.price} desc)
      `).withNull(false).format(Number),

			// 累计销售额（需要先join订单项表）
			cumulativeSales: createColumn(sql`
        sum(0) over (order by ${p.id} rows unbounded preceding)
      `).withNull(true).format(Number),
		})),
}

// 4. 复杂聚合查询
export const complexAggregationQueries = {
	// 产品销售统计（多维度）
	productSalesStats: productTable
		.andWhere((p) => sql`${p.isActive} = ${true}`) // 在groupBy前过滤
		.join('left join', orderItemTable)
		.on(({ base, extra }) => sql`${base.id} = ${extra.productId}`)
		.groupBy(
			({ base }) => ({
				productId: base.id,
				productName: base.name,
				categoryId: base.categoryId
			}),
			(createColumn) => ({
				totalSold: createColumn((template) => sql`coalesce(sum(${template.extra.quantity}), 0)`)
					.withNull(false)
					.format(Number),

				totalRevenue: createColumn((template) => sql`coalesce(sum(${template.extra.quantity} * ${template.extra.unitPrice}), 0)`)
					.withNull(false)
					.format(Number),

				avgOrderSize: createColumn((template) => sql`coalesce(avg(${template.extra.quantity}), 0)`)
					.withNull(false)
					.format(Number),

				orderCount: createColumn((template) => sql`count(distinct ${template.extra.orderId})`)
					.withNull(false)
					.format(Number),

				// 计算市场份额（相对于同类产品）
				marketShare: createColumn((template) => sql`
          round(
            coalesce(sum(${template.extra.quantity} * ${template.extra.unitPrice}), 0) * 100.0 / 
            nullif(sum(sum(${template.extra.quantity} * ${template.extra.unitPrice})) over (partition by ${template.base.categoryId}), 0),
            2
          )
        `).withNull(true).format(Number),
			})
		)
}

// 5. 性能优化查询
export const performanceOptimizedQueries = {
	// 使用LATERAL JOIN优化复杂查询
	optimizedProductSales: productTable
		.lateralJoin('left join lazy withNull', (p) =>
			orderItemTable
				.andWhere((oi) => sql`${oi.productId} = ${p.id}`)
				.groupBy(
					(oi) => ({ productId: oi.productId }),
					(createColumn) => ({
						totalSold: createColumn(() => sql`sum(0)`)
							.withNull(false)
							.format(Number),
						totalRevenue: createColumn(() => sql`sum(0)`)
							.withNull(false)
							.format(Number),
					})
				)
		)
		.on(({ base, extra }) => sql`${base.id} = ${extra.keys.productId}`),

	// 分页优化 - 使用游标分页
	cursorBasedPagination: (cursor?: number, limit: number = 20) => {
		let query = productTable.order('ASC', 'FIRST', (p) => p.id)

		if (cursor) {
			query = query.andWhere((p) => sql`${p.id} > ${cursor}`)
		}

		return query.take(limit)
	},
}


// 6. 数据验证和格式化
export const dataValidationQueries = {
	// 带数据验证的产品查询
	validatedProducts: productTable
		.mapTo((p, { createColumn }) => ({
			...p,
			// 价格格式化
			formattedPrice: createColumn(sql`to_char(${p.price}, 'FM999,999.00')`)
				.withNull(false)
				.format(String),

			// 库存状态
			stockStatus: createColumn(sql`case
        when ${p.stock} = 0 then 'out_of_stock'
        when ${p.stock} < 10 then 'low_stock'
        when ${p.stock} < 50 then 'medium_stock'
        else 'high_stock'
      end`)
				.withNull(false)
				.format(String),

			// 价格等级
			priceTier: createColumn(sql`case
        when ${p.price} < 50 then 'budget'
        when ${p.price} < 200 then 'mid_range'
        when ${p.price} < 1000 then 'premium'
        else 'luxury'
      end`)
				.withNull(false)
				.format(String),
		})),
}

// 7. 批量操作示例
export const batchOperationQueries = {
	// 批量更新产品状态
	updateProductStatus: (productIds: number[], isActive: boolean) =>
		sql`update products set is_active = ${isActive} where id = any(${productIds})`,

	// 批量插入示例（需要配合原生SQL）
	insertProducts: (products: Array<{
		name: string
		categoryId: number
		price: number
		stock: number
	}>) => {
		const values = products.map(p =>
			sql`(${p.name}, ${p.categoryId}, ${p.price}, ${p.stock})`
		).join(', ')

		return sql`insert into products (name, category_id, price, stock) values ${values}`
	},
}

// 8. 使用示例
export async function runAdvancedExamples() {
	const adapter = SqlAdapter.createPostgresAdapter()

	const sqlQuery = async <R>(getBundle: (adapter: SqlAdapter) => SqlExecuteBundle<R>) => {
		const bundle = getBundle(adapter)
		console.log('模拟执行', bundle.sql, bundle.paramArr)
		const results = [] as { [key: string]: unknown }[]
		return await bundle.formatter(results)
	}

	try {
		// 复杂聚合查询
		const salesStats = await sqlQuery((adapter) => adapter.selectAll(complexAggregationQueries.productSalesStats))
		console.log('产品销售统计:', salesStats)

		// 窗口函数查询
		const rankings = await sqlQuery((adapter) => adapter.selectAll(windowFunctionQueries.productRankings))
		console.log('产品排名:', rankings)

		// 游标分页
		const firstPage = await sqlQuery((adapter) => adapter.selectAll(
			performanceOptimizedQueries.cursorBasedPagination(undefined, 10)
		))
		console.log('第一页产品:', firstPage)

		const lastId = firstPage[firstPage.length - 1]?.id
		if (lastId) {
			const secondPage = await sqlQuery((adapter) => adapter.selectAll(
				performanceOptimizedQueries.cursorBasedPagination(lastId, 10)
			))
			console.log('第二页产品:', secondPage)
		}

	} catch (error) {
		console.error('高级查询执行错误:', error)
	}
}
