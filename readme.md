使用ts写法构建sql,用法参考`scripts/test.ts`

# hint
```ts
view.pipe((v)=>v.join(v))
```
导致viewTemplate不能在构建阶段初始化

# hint
```ts
const column = createColumn(`1`)
createView(()=>column).join(createView(()=>column))
```
导致createColumn需要在运行期提供

# hint
```
view.andWhere((sql,e)=>`exists(${view.andWhere((sql2,e2)=>``)})`)
```
`sql`重复,故`sql`为全局函数,`sqlSegment`为构建期概念

# hint

```
createSqlView(sql`xxx`,()=>{
	return {
		//....
	}
}).laterJoin(otherView,(t)=>createSqlView(sql`select * from yyy where ${t.columnA}=1`,()=>{
	return {
		//...
	}
}))
```
`sqlSegment`本身是构建期概念,但内部能包含运行期才产生的column


# hint
```ts
view
	.andWhere((e1)=>sql`${e1.columnA} = ${1}`)
	.andWhere((e1)=>`exists (${
		view.andWhere((e2)=>sql`${e1.columnA} = ${e2.columnA}`)
	})`)
```
此语法存在,根据
- column是运行期构建,需要在declareUsed后才能开始获取实际ref
- view内部存在column,需要传递declareUsed

说明转sql返回值是某种数据结构而非函数以便传递declareUsed

```ts
const sqlSegment1 =sql`${}`//[ `string`,column, view, sqlSegment]

const builderArr = sqlSegment1.map((e)=>e.getBuilder())
builderArr.forEach((e)=>e.declareUsed())
//otherSegment.declareUsed()
return builderArr.map((e)=>e.getString()).join(join)
```

- view内部可能存在column,需要传递declareUsed
view内部可能有父column,不能在sql构造时立刻构造viewCtx并declareUsedAllSelect,而是语句declareUsed时构造viewCtx

引出概念`SqlSegment.getBuilder()`

`sqlStruct`内存储`builder`  


# 工作流程
- emitDeclareUsedColumn
- getSqlStruct
- 提供ctx,确定最终分配的alias
- columnExpr对比,构建select部分
