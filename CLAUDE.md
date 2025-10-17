# Typed SQL View - 类型安全的SQL视图构建库

## 核心思想

`typed-sql-view` 是一个基于 TypeScript 的类型安全 SQL 查询构建库，其核心思想是将 SQL 查询抽象为可组合的、类型安全的视图对象。主要特点包括：

### 1. 类型安全的SQL构建
- 使用 TypeScript 的类型系统确保查询结果的类型安全
- 编译时就能发现类型错误，避免运行时错误
- 支持复杂的嵌套查询和关联查询

### 2. 可组合的视图设计
- 每个 SQL 查询都是一个 `SqlView` 对象
- 视图可以链式组合，支持条件过滤、关联查询、分组聚合等操作
- 支持延迟执行和按需加载

### 3. 数据库适配器模式
- 内置 MySQL 和 PostgreSQL 适配器
- 可自定义适配器支持其他数据库
- 自动处理参数化查询和SQL注入防护

### 4. 函数式编程风格
- 使用函数式编程范式构建查询
- 支持高阶函数和回调函数
- 代码简洁易读

## 核心概念

### SqlView
`SqlView` 是库的核心类，代表一个可执行的 SQL 查询。每个 `SqlView` 都支持链式调用各种查询方法，如 `andWhere`、`join`、`groupBy`、`order` 等。

### SqlView生命周期

SqlView有两个主要阶段：

#### 1. 声明期（Declaration Phase）
- 用户调用 `createSqlView`、`join`、`groupBy` 等方法时
- 此时创建的是 `SqlView` 对象，包含构建器函数
- 在这个阶段，`template` 还不存在，只有构建器函数

#### 2. 构建期（Build Phase）
- 调用 `adapter.selectAll()` 等方法时触发
- 创建 `SelectStructBuilder` 并构建实际的 SQL
- 此时 `template` 才被创建，包含实际的 `ColumnRef` 对象

### ColumnRef
`ColumnRef` 表示一个数据库列，支持以下方法：
- `withNull(boolean)`: 设置是否允许null值
- `format(function)`: 设置数据格式化函数

### createColumn函数的不同签名

`createColumn` 函数在不同上下文中有不同的签名：

#### 1. createSqlView中的createColumn
```typescript
// 签名：(getSegment: (rootAlias: Segment) => Segment) => ColumnRef
createColumn: (getSegment: (rootAlias: Segment) => Segment) => ColumnRef<boolean, unknown>
```

#### 2. groupBy中的createColumn
```typescript
// 签名：(getSegment: (vt: VT1) => Segment) => ColumnRef
createColumn: (getSegment: (vt: VT1) => Segment) => ColumnRef<boolean, unknown>
```

#### 3. mapTo中的createColumn
```typescript
// 签名：(getSegment: Segment) => ColumnRef
createColumn: (getSegment: Segment) => ColumnRef
```

### Segment
`Segment` 是 SQL 片段的基础抽象类，用于构建复杂的 SQL 表达式。使用`sql`或`rawSql`函数创建

## 基本用法

### 1. 创建表视图

```typescript
import { createSqlView, sql } from '@millionkn/typed-sql-view'

// 创建用户表视图
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
  }
})
```

### 2. 条件查询

```typescript
// 添加WHERE条件
const activeUsers = userTable
  .andWhere((u) => sql`${u.name} like ${'%admin%'}`)
  .andWhere((u) => sql`${u.id} > ${100}`)

// 支持复杂的子查询
const usersWithOrders = userTable
  .andWhere((u) => sql`exists (
    ${orderTable
      .andWhere((o) => sql`${o.userId} = ${u.id}`)
      .mapTo(() => []) // 转换为 select 1
    }
  )`)
```

### 3. 关联查询

```typescript
// 创建订单表视图
const orderTable = createSqlView(sql`"orders"`, (column) => {
  return {
    id: column((rootAlias) => sql`${rootAlias}."id"`).withNull(false).format(Number),
    userId: column((rootAlias) => sql`${rootAlias}."user_id"`).withNull(false).format(Number),
    amount: column((rootAlias) => sql`${rootAlias}."amount"`).withNull(false).format(Number),
  }
})

// LEFT JOIN
const usersWithOrders = userTable
  .join('left join', orderTable)
  .on(({ base, extra }) => sql`${base.id} = ${extra.userId}`)

// INNER JOIN
const usersWithActiveOrders = userTable
  .join('inner join', orderTable)
  .on(({ base, extra }) => sql`${base.id} = ${extra.userId}`)
```

### 4. 分组聚合

```typescript
// 按用户分组统计订单
const userOrderStats = userTable
  .groupBy(
    (u) => ({ userId: u.id, userName: u.name }), // 分组字段
    (createColumn) => ({ // 聚合字段
      orderCount: createColumn(() => sql`count(*)`).withNull(false).format(Number),
      totalAmount: createColumn(() => sql`sum(${orderTable.amount})`).withNull(true).format(Number),
    })
  )
```

### 5. 排序和分页

```typescript
const paginatedUsers = userTable
  .order('DESC', 'LAST', (u) => u.id) // 按ID降序，null值排在最后
  .skip(10) // 跳过前10条
  .take(20) // 取20条
```

### 6. 数据转换

```typescript
// 使用mapTo转换查询结果
const userSummary = userTable
  .mapTo((u, { createColumn }) => ({
    displayName: createColumn(sql`concat(${u.name}, ' (', ${u.email}, ')')`)
      .withNull(false)
      .format(String),
    isActive: createColumn(sql`${u.id} > 0`)
      .withNull(false)
      .format(Boolean),
  }))
```

#### 特殊用法：转换为 select 1

当需要`select 1`时，可以使用`mapTo(() => [])`：

```typescript
// 用于EXISTS子查询
const usersWithOrders = userTable
  .andWhere((u) => sql`exists (
    ${orderTable
      .andWhere((o) => sql`${o.userId} = ${u.id}`)
      .mapTo(() => []) // 转换为 select 1
    }
  )`)

// 用于聚合查询
const orderCount = orderTable
  .andWhere((o) => sql`${o.status} = ${'completed'}`)
  .groupBy(() => ({}), (createColumn) => ({
    count: createColumn(() => sql`count(*)`).withNull(false).format(Number)
  }))
  .mapTo((result) => result.aggrateValues.count)
```

## 高级用法

### 1. 横向关联 (Lateral Join)

```typescript
// 为每个用户获取最新的订单
const usersWithLatestOrder = userTable
  .lateralJoin('left join lazy withNull', (u) => 
    orderTable
      .andWhere((o) => sql`${o.userId} = ${u.id}`)
      .order('DESC', 'LAST', (o) => o.id)
      .take(1)
  )
  .on(({ base, extra }) => sql`${base.id} = ${extra.userId}`)
```

### 2. 嵌套查询

```typescript
// 子查询作为表使用
const subQuery = userTable
  .andWhere((u) => sql`${u.id} > ${100}`)
  .mapTo((u) => ({ id: u.id, name: u.name }))

const nestedQuery = subQuery
  .join('left join', orderTable)
  .on(({ base, extra }) => sql`${base.id} = ${extra.userId}`)
```

### 3. 条件括号化

```typescript
// 控制查询的括号化
const complexQuery = userTable
  .bracketIf(({ state }) => state.has('groupBy') || state.has('having'))
  .groupBy((u) => ({ name: u.name }), (createColumn) => ({
    count: createColumn(() => sql`count(*)`).withNull(false).format(Number)
  }))
```

#### 自动括号化原理

库会自动在需要的时候为查询添加括号，以确保SQL语法的正确性。这个机制基于查询的状态（state）来决定是否需要括号化：

- **何时需要括号化**：
  - 包含 `GROUP BY` 或 `HAVING` 子句的查询
  - 包含 `ORDER BY`、`LIMIT`、`OFFSET` 的复杂查询
  - 在 `JOIN` 操作中，根据连接类型决定括号化策略

- **括号化策略**：
  - `LEFT JOIN`：当包含 `INNER JOIN`、`WHERE`、`GROUP BY`、`HAVING`、`ORDER BY`、`LIMIT`、`OFFSET` 时括号化
  - `INNER JOIN`：当包含 `LEFT JOIN`、`GROUP BY`、`HAVING`、`ORDER BY`、`LIMIT`、`OFFSET` 时括号化

- **手动控制**：
  ```typescript
  // 自定义括号化条件
  const customQuery = userTable
    .bracketIf(({ state }) => state.has('groupBy') || state.has('order'))
    .groupBy((u) => ({ name: u.name }), (createColumn) => ({
      count: createColumn(() => sql`count(*)`).withNull(false).format(Number)
    }))
  ```

## 数据库适配器

### 使用内置适配器

```typescript
import { SqlAdapter } from '@millionkn/typed-sql-view'

// PostgreSQL适配器
const pgAdapter = SqlAdapter.createPostgresAdapter()

// MySQL适配器  
const mysqlAdapter = SqlAdapter.createMySqlAdapter()

// 执行查询
const result = await pgAdapter.selectAll(userTable)
```

### 自定义适配器

```typescript
const customAdapter = new SqlAdapter({
  paramHolder: (index) => `?`, // 参数占位符
  adapter: {
    tableAlias: (alias) => `"${alias}"`, // 表别名格式
    columnRef: (tableAlias, columnAlias) => `"${tableAlias}"."${columnAlias}"`, // 列引用格式
    selectAndAlias: (select, alias) => `${select} as "${alias}"`, // SELECT和别名格式
    pagination: (skip, take) => { // 分页语法
      let result: string[] = []
      if (skip > 0) result.push(`offset ${skip}`)
      if (take !== null) result.push(`limit ${take}`)
      return result.join(' ')
    },
    order: (items) => `order by ${items.map(({ expr, order, nulls }) => 
      `${expr} ${order} nulls ${nulls}`
    ).join(', ')}`, // 排序语法
  }
})
```

## 执行查询

### 查询方法

```typescript
const adapter = SqlAdapter.createPostgresAdapter()

// 查询多条记录
const users: User[] = await adapter.selectAll(userTable)

// 查询单条记录
const user: User | null = await adapter.selectOne(userTable)

// 聚合查询
const totalCount: number = await adapter.selectTotal(userTable)

// 自定义聚合
const customAgg = await adapter.aggrateView(userTable, (createColumn) => ({
  avgId: createColumn((u) => sql`avg(${u.id})`).withNull(true).format(Number),
  maxId: createColumn((u) => sql`max(${u.id})`).withNull(true).format(Number),
}))
```

### 获取SQL和参数

```typescript
const bundle = adapter.selectAll(userTable)

console.log('SQL:', bundle.sql)
console.log('参数:', bundle.paramArr)

// 手动执行
const results = await db.query(bundle.sql, bundle.paramArr)
const formattedResults = await bundle.formatter(results)
```

## 注意事项

### 1. 类型安全
- 始终使用 `withNull()` 明确指定列是否允许null值
- 使用 `format()` 函数确保数据类型正确
- 利用 TypeScript 的类型推导获得完整的类型安全

### 2. 性能考虑
- 使用 `lazy` join 模式避免不必要的查询
- 合理使用 `bracketIf()` 控制查询复杂度
- 考虑数据库索引对查询性能的影响

### 3. 数据库兼容性
- 不同数据库的SQL语法可能有差异
- 使用对应的适配器确保兼容性
- 测试不同数据库的行为差异

### 4. 错误处理
- 格式化函数中的错误会被传播
- 使用 try-catch 处理查询执行错误
- 考虑使用 Zod 等库进行数据验证

### 5. 内存管理
- 大型查询可能消耗较多内存
- 考虑使用流式处理或分页查询
- 及时释放不需要的查询对象

## 最佳实践

### 1. 查询结构设计
```typescript
// 好的做法：清晰的命名和结构
const userView = createSqlView(sql`"users"`, (column) => ({
  id: column((root) => sql`${root}."id"`).withNull(false).format(Number),
  name: column((root) => sql`${root}."name"`).withNull(false).format(String),
}))

// 避免：复杂的嵌套和混乱的命名
```

### 2. 类型定义
```typescript
// 定义清晰的接口
interface User {
  id: number
  name: string
  email: string | null
}

// 使用类型断言确保类型安全
const users: User[] = await adapter.selectAll(userView)
```

### 3. 查询组合
```typescript
// 创建可复用的查询构建器
const createUserQuery = (filters: UserFilters) => {
  let query = userView
  
  if (filters.name) {
    query = query.andWhere((u) => sql`${u.name} like ${`%${filters.name}%`}`)
  }
  
  if (filters.activeOnly) {
    query = query.andWhere((u) => sql`${u.active} = true`)
  }
  
  return query
}
```

这个库提供了强大而灵活的类型安全SQL查询能力，通过合理的组合和设计，可以构建出复杂而高效的数据库查询逻辑。

## 示例文件

为了更好地理解和使用这个库，我们提供了三个详细的示例文件：

### 1. 基本用法示例 (`examples/basic-usage.ts`)
包含以下内容：
- 表视图创建
- 基本查询操作
- 关联查询 (JOIN)
- 条件过滤
- 数据转换
- 分页和排序
- 完整的执行示例

### 2. 高级用法示例 (`examples/advanced-usage.ts`)
包含以下内容：
- 复杂表结构和递归查询
- 窗口函数应用
- 复杂聚合查询
- 性能优化技巧
- 查询构建器模式
- 数据验证和格式化
- 批量操作示例

### 3. 自定义适配器示例 (`examples/custom-adapter.ts`)
包含以下内容：
- SQLite 适配器实现
- Oracle 适配器实现
- SQL Server 适配器实现
- 自定义语法适配器
- 扩展适配器功能
- 数据库特定优化
- 查询钩子和缓存

## 快速开始

1. 查看 `examples/basic-usage.ts` 了解基本用法
2. 参考 `examples/advanced-usage.ts` 学习高级特性
3. 根据需要使用 `examples/custom-adapter.ts` 中的适配器
4. 在实际项目中应用这些模式和技巧

这些示例文件提供了完整的、可运行的代码，可以直接复制到你的项目中使用。
