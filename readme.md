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
导致createColumn需要在构建期提供

# hint
```
view.andWhere((sql,e)=>`exists(${view.andWhere((sql2,e2)=>``)})`)
```
`sql`重复,故`sql`为全局函数,`sqlSegment`为声明期概念

# hint

```
createSqlView(sql`xxx`,()=>{
	return {
		//....
	}
}).lateralJoin(otherView,(t)=>createSqlView(sql`select * from yyy where ${t.columnA}=1`,()=>{
	return {
		//...
	}
}))
```
`sqlSegment`本身是声明期概念,但内部能包含构建期才产生的column(通过闭包)


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
- 复用需求

declareUsed分为两个阶段:
- emitSelectColumnUsed
- emitSegmentUsed

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
- emitExportUsed,
- emitInnerUsed,
- getSqlStruct
- getExpr
- getExprStr

由于lazy join,`emitExportUsed`可能会影响`emitInnerUsed`,故`emitExportUsed`先于`emitInnerUsed`

例:
```ts
xxx.lateralJoin((e)=>{
	return createSqlView(sql`${e.lazyColumn}`)
})
```
对于`base`来说,此时e上的任何`column`都是`export`,而后续pipe不论如何使用(`select`,`where`等),都是由此处的`export`衍生来的

由于`lazyColumn`存在,导致lateralJoin内部,extra必须先于base全部声明,又由于`export`先于`inner`,故声明顺序为
- `extra-export`
- `extra-inner`
- `base-export`
- `base-inner`

在这个顺序中,`base-export`的声明其实被分散到了`extra-inner`和`base-export`,而更早的 `extra-export`对于`base`没有影响,因此在`base`看来,声明顺序仍然是`base-export`->`base-inner`,只不过`base-outer`中进行了重复的声明

ps:`base-export`不会影响`extra-pure-inner`,而`base-export-in-extra`和`base-export`不分先后,故`extra-inner`和`base-export`可以调换顺序