// scalastyle:off
package cn.juntaozhang.example

import cn.juntaozhang.example.spark.Person
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class LogicalPlanSpec extends AnyFunSuite with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession.builder()
    .appName("LogicalPlanTest")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  import spark._
  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  test("concat") {
    //sql("select concat('Spark', 'SQL')").show()
    sql(" select 'Spark' || 'SQL' AS name ").show()
  }

  test("create hive table") {
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW sales_table AS
        |    SELECT * FROM VALUES
        |    ("A", 20210101, 100),
        |    ("A", 20210102, 200),
        |    ("B", 20210101, 300),
        |    ("A", 20210103, 150),
        |    ("B", 20210102, 400),
        |    ("A", 20210105, 50),
        |    ("B", 20210104, 50)
        |    AS sales_table(category, dt, sales)
        |""".stripMargin)
    sql("select * from sales_table").write.mode(SaveMode.Overwrite).saveAsTable("sales_table")

    Seq(
      ("Alice", 29L, "New York"),
    ).toDF("name", "age", "city").createOrReplaceTempView("people")
    sql("select * from people").write.mode(SaveMode.Overwrite).saveAsTable("people")
  }

  test("logical plan") {
    val query = "SELECT name, age FROM people WHERE salary > 50000"

    // 解析查询
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)
    println(logicalPlan)

    // 分析查询
    val analyzedPlan = spark.sessionState.analyzer.execute(logicalPlan)
    println(analyzedPlan)

    val optimizedPlan = spark.sessionState.optimizer.execute(analyzedPlan)
    println(optimizedPlan)
  }

  test("logical plan2") {
    val query = "SELECT name, age FROM people WHERE age > 30"

    // 解析查询
    val logicalPlan = spark.sessionState.sqlParser.parsePlan(query)
    println(logicalPlan)

    // 分析查询
    val analyzedPlan = spark.sessionState.analyzer.execute(logicalPlan)
    println(analyzedPlan)
  }

  test("canonicalize 标准化") {
    // 创建一个简单的LogicalPlan
    val plan1 = Project(Seq(Alias(Add(Literal(1), AttributeReference("a", IntegerType)()), "result")()), LocalRelation(AttributeReference("a", IntegerType)()))

    val plan2 = Project(Seq(Alias(Add(AttributeReference("a", IntegerType)(), Literal(1)), "result")()), LocalRelation(AttributeReference("a", IntegerType)()))

    // 打印原始逻辑计划
    println(s"Original Plan 1: ${plan1}")
    println(s"Original Plan 2: ${plan2}")

    // 规范化逻辑计划
    val canonicalizedPlan1 = plan1.canonicalized
    val canonicalizedPlan2 = plan2.canonicalized

    // 打印规范化后的逻辑计划
    println(s"Canonicalized Plan 1: ${canonicalizedPlan1}")
    println(s"Canonicalized Plan 2: ${canonicalizedPlan2}")

    // 比较规范化后的逻辑计划
    println(s"Plans are equal: ${canonicalizedPlan1 == canonicalizedPlan2}")
    println(s"Plans are equal: ${canonicalizedPlan1 canEqual canonicalizedPlan2}")
  }

  // TODO: not working
  test("eliminate subquery => EliminateSubqueryAliases") {
    val query = (
      """
        |SELECT *
        |FROM (
        |  SELECT category, sales
        |  FROM sales_table
        |) AS subquery
        |WHERE subquery.category = 'A';
        |""".stripMargin)
    sql(query).explain(true)
  }

  test("Window") {
    sql(
      """
        |select * from sales_table
        |""".stripMargin).show()


    sql(
      """
        |SELECT
        |  category,
        |  sales,
        |  SUM(sales) OVER (
        |      PARTITION BY category ORDER BY sales DESC
        |      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        |  ) as cumulative_sales
        |FROM
        |  sales_table order by sales DESC
        |""".stripMargin).show()


    sql(
      """
        |SELECT
        |  category,
        |  sales,
        |  SUM(sales) OVER (
        |      PARTITION BY category ORDER BY sales DESC
        |      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        |  ) as cumulative_sales
        |FROM
        |  sales_table order by sales DESC
        |""".stripMargin).show()
  }

  test("ROWS BETWEEN vs RANGE BETWEEN") {
    sql(
      """
        |SELECT
        |  category,
        |  dt,
        |  sales,
        |  sum(sales) OVER (
        |      PARTITION BY category ORDER BY dt asc
        |      ROWS BETWEEN 0 PRECEDING AND 1 FOLLOWING
        |  ) as rank
        |FROM
        |  sales_table
        |""".stripMargin).show()

    sql(
      """
        |SELECT
        |  category,
        |  dt,
        |  sales,
        |  sum(sales) OVER (
        |      PARTITION BY category ORDER BY dt asc
        |      RANGE BETWEEN 0 PRECEDING AND 1 FOLLOWING
        |  ) as rank
        |FROM
        |  sales_table
        |""".stripMargin).show()
  }

  test("PullOutNondeterministic") {
    // PullOutNondeterministic规则的主要目的是为了确保聚合操作的一致性和正确性。
    // 在聚合查询中，如果聚合键（即在GROUP BY子句中指定的列）包含非确定性表达式，
    // 那么同一个键可能会在每次查询执行时产生不同的值，从而导致聚合结果不一致。

    // 所以会先放到project中使其确定，然后再进行聚合操作，见PullOutNondeterministic.scala
    val df = sql(
      """
        |SELECT category, SUM(sales)
        |FROM sales_table
        |GROUP BY category, rand()
        |""".stripMargin)
    df.explain(true)
    df.show()
  }

  test("udf HandleNullInputsForUDF null case1") {
    val data = Seq(
      Row("zhangsan", 20),
      Row("lisi", null),
      Row("wangwu", 30)
    )

    val schema = StructType(Array(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = true)
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    spark.udf.register("add", (age: Int) => age + 1)
    val result = df.withColumn("age2", expr("add(age)"))
    result.show()
    // add('age) AS age2 -> if (isnull(age#22)) null else add(knownnotnull(age#22)) AS age2
    result.explain(true)
  }

  test("udf HandleNullInputsForUDF null case2") {
    import org.apache.spark.sql.functions.udf
    val add = udf((s: Int) => {
      s + 1
    })
    val df = Seq(
      Person("zhangsan", 20),
      Person("lisi", null),
      Person("wangwu", 30)
    ).toDF()

    val result = df.withColumn("age2", add($"age"))
    result.show()
    df.explain(true)
  }


  test("UpdateAttributeNullability") {
    val usersData = Seq(
      Row(1, "张三"),
      Row(2, "李四"),
      Row(3, "王五")
    )
    val usersSchema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))
    val usersDF = spark.createDataFrame(
      spark.sparkContext.parallelize(usersData),
      usersSchema
    )

    val ordersData = Seq(
      Row(100, 1),
      Row(101, 2),
      Row(102, null)
    )
    val ordersSchema = StructType(Array(
      StructField("order_id", IntegerType, nullable = true),
      StructField("user_id", IntegerType, nullable = true)
    ))
    val ordersDF = spark.createDataFrame(
      spark.sparkContext.parallelize(ordersData),
      ordersSchema
    )

    // 执行左外连接操作
    val joinDF = ordersDF.join(
      usersDF, usersDF("id") === ordersDF("user_id"), "left_outer"
    ).limit(10)
    // val optimizedPlan = joinDF.limit(10).queryExecution.optimizedPlan
    joinDF.show()
    joinDF.explain(true)
  }


  test("ReplaceExpressions") {
    // Left 继承了RuntimeReplaceable 使用replacement(Substring) 替换
    val df = Seq("Spark", "SQL", "Example").toDF("word").selectExpr("left(word, 2) as shortened")
    df.show()
    df.explain(true)
  }

  test("PullOutGroupingExpressions") {
    Seq(
      Person("zhangsan", 20),
      Person("lisi", null),
      Person("wangwu", 30)
    ).toDF().createOrReplaceTempView("t")

    val df = sql("SELECT NOT(age IS NULL) FROM t GROUP BY age IS NULL")
    df.show()
    df.explain(true)
    /*
    == Optimized Logical Plan ==
    Aggregate [_groupingexpression#38], [NOT _groupingexpression#38 AS (NOT (age IS NULL))#36]
    +- LocalRelation [_groupingexpression#38]
    确保不会有很复杂的表达式在GroupBy中
     */

    // expression 't.age' is neither present in the group by, nor is it an aggregate function.
    // sql("SELECT IsNotNull(age), COUNT(*) FROM t GROUP BY age IS NULL").show()
  }

  test("RemoveNoopOperators") {
    val df = sql(
      """
        |select name from (
        | select name from people
        |) as sub_query
        |""".stripMargin)
    df.show()
    df.explain(true)
  }

  test("RewriteDistinctAggregates") {
    val data = Seq(
      ("a", "ca1", "cb1", 10),
      ("a", "ca1", "cb2", 5),
      ("b", "ca1", "cb1", 13))
      .toDF("key", "cat1", "cat2", "value")
    data.createOrReplaceTempView("data")

    val agg = data.groupBy($"key")
      .agg(
        count_distinct($"cat1").as("cat1_cnt"),
        count_distinct($"cat2").as("cat2_cnt"),
        sum($"value").as("total")
      )

    agg.explain(true)
  }

  test("EliminateMapObjects") {
    val df = spark.createDataFrame(Seq(
      (1, "a"),
      (2, "b"),
      (3, "c")
    )).toDF("id", "value")

    val upperDF = df.select(col("id"), upper(col("value")).alias("value_upper"))
    upperDF.explain(true)
  }

  test("PushDown") {
    spark.createDataFrame(Seq(
      (1, "a"),
      (2, "b"),
      (3, "c")
    )).toDF("id", "value").createOrReplaceTempView("t1")

    spark.createDataFrame(Seq(
      (1, "a"),
      (2, "b"),
      (3, "c")
    )).toDF("id", "value").createOrReplaceTempView("t2")

    val df = sql(
      """
        |select * from (
        | select id from t1
        | union all
        | select id from t2
        |) where id > 1
        |""".stripMargin)

    df.explain(true)
  }

  test("EliminateOuterJoin") {
    spark.createDataFrame(Seq(
      (1, "a"),
      (2, "b"),
      (3, "c")
    )).toDF("id", "value").createOrReplaceTempView("t1")

    spark.createDataFrame(Seq(
      (1, "a"),
      (2, "b"),
      (3, "c")
    )).toDF("id", "value").createOrReplaceTempView("t2")

    val df = sql(
      """
        |select DISTINCT t1.id from t1 left join t2 on t1.id = t2.id
        |""".stripMargin)

    df.explain(true)
  }

  test("LimitPushDown") {
    spark.createDataFrame(Seq(
      (1, "a"),
      (2, "b"),
      (3, "c")
    )).toDF("id", "value").createOrReplaceTempView("t1")

    spark.createDataFrame(Seq(
      (1, "a"),
      (2, "b"),
      (3, "c")
    )).toDF("id", "value").createOrReplaceTempView("t2")

    val df = sql(
      """
        |select t1.id from t1 left join t2 on t1.id = t2.id
        |""".stripMargin)

    df.explain(true)
  }

}
// scalastyle:on