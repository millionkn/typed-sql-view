# Typed SQL View 示例文件

这个目录包含了 `@millionkn/typed-sql-view` 库的详细使用示例。

## 文件说明

### 1. `basic-usage.ts` - 基本用法示例
适合初学者，展示了库的核心功能：
- 如何创建表视图
- 基本的查询操作
- 简单的关联查询
- 条件过滤和排序
- 数据格式化

**运行方式：**
```bash
# 确保已安装依赖
npm install @millionkn/typed-sql-view zod

# 编译并运行
npx ts-node examples/basic-usage.ts
```

### 2. `advanced-usage.ts` - 高级用法示例
适合有经验的开发者，展示了复杂场景：
- 递归查询（层级数据）
- 窗口函数
- 复杂聚合
- 性能优化
- 查询构建器模式
- 数据验证

**运行方式：**
```bash
npx ts-node examples/advanced-usage.ts
```

### 3. `custom-adapter.ts` - 自定义适配器示例
展示如何为不同数据库创建适配器：
- SQLite 适配器
- Oracle 适配器  
- SQL Server 适配器
- 自定义语法适配器
- 扩展功能适配器

**运行方式：**
```bash
npx ts-node examples/custom-adapter.ts
```

## 学习建议

1. **从基础开始**：先运行 `basic-usage.ts`，理解核心概念
2. **深入高级特性**：学习 `advanced-usage.ts` 中的复杂用法
3. **自定义适配器**：根据你的数据库需求，参考 `custom-adapter.ts`
4. **实际应用**：将示例中的模式应用到你的项目中

## 注意事项

- 示例中的数据库表结构是虚构的，实际使用时需要根据你的数据库调整
- 某些高级特性可能需要特定数据库版本支持
- 示例代码可以直接复制到你的项目中使用
- 建议在实际使用前先测试查询的 SQL 输出

## 扩展示例

如果你想贡献更多示例，欢迎：
1. 创建新的示例文件
2. 在现有示例中添加更多场景
3. 优化和修正现有代码
4. 添加更多数据库适配器示例
