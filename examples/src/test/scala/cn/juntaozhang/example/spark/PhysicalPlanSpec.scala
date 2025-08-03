// scalastyle:off
package cn.juntaozhang.example.spark

import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning}
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PhysicalPlanSpec extends AnyFunSuite with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession.builder()
    .appName("LogicalPlanTest")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  import spark._
  import spark.implicits._

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  test("create hive table") {
    sql("drop table if exists counts_table")
    sql("drop table if exists sales_table")
    sql(
      """
        |CREATE OR REPLACE TEMP VIEW sales_table AS
        |    SELECT * FROM VALUES
        |    (1, "A", "c1" ,20210101, 100),
        |    (2, "A", "c1" ,20210102, 200),
        |    (3, "B", "c1" ,20210101, 300),
        |    (4, "A", "c1" ,20210103, 150),
        |    (5, "B", "c2" ,20210102, 400),
        |    (6, "A", "c2" ,20210105, 50),
        |    (7, "B", "c2" ,20210104, 50),
        |    (9, "B", "c2" ,20210106, 100)
        |    AS sales_table(id, category,c2, dt, sales)
        |""".stripMargin)
    sql("select * from sales_table").write.mode(SaveMode.Overwrite).saveAsTable("sales_table")

    sql(
      """
        |CREATE OR REPLACE TEMP VIEW counts_table AS
        |    SELECT * FROM VALUES
        |    (1, "A", 20210101, 1),
        |    (2, "A", 20210102, 2),
        |    (3, "B", 20210101, 3),
        |    (4, "A", 20210103, 1),
        |    (5, "B", 20210102, 4),
        |    (6, "A", 20210105, 2),
        |    (7, "B", 20210104, 2),
        |    (8, "B", 20210105, 1)
        |    AS counts_table(id, category, dt, counts)
        |""".stripMargin)
    sql("select * from counts_table").write.mode(SaveMode.Overwrite).saveAsTable("counts_table")

    sql("drop table if exists fact_table")
    sql("drop table if exists dim_table")
    val factData = Seq(
      (1, "2023-01-01", 100),
      (2, "2023-01-02", 200),
      (3, "2023-01-03", 300)
    ).toDF("id", "date", "value")

    val dimData = Seq(
      ("2023-01-01", "New Year"),
      ("2023-01-02", "Day After New Year")
    ).toDF("date", "event")

    factData.write.partitionBy("date").mode("overwrite").saveAsTable("fact_table")
    dimData.write.mode("overwrite").saveAsTable("dim_table")
  }

  test("partition satisfies diff source") {
    val df1 = Seq(
      (3, "c", 300)
    ).toDF("key1", "key2", "value1")

    val df2 = Seq(
      (3, 700)
    ).toDF("key1", "value2")

    val partitioning1 = HashPartitioning(df2.col("key1").expr :: Nil, numPartitions = 3)
    val distribution = ClusteredDistribution(Seq(df1.col("key1").expr), requireAllClusterKeys = false)
    val satisfies1 = partitioning1.satisfies(distribution)

    println(s"Satisfies1: $satisfies1") // 输出：Satisfies1: false
  }

  test("partition satisfies partial partition key") {
    val df1 = Seq(
      (3, "c", 300)
    ).toDF("key1", "key2", "value1")

    val df2 = Seq(
      (3, 700)
    ).toDF("key1", "value2")

    val partitioning1 = HashPartitioning(df1.col("key1").expr :: df1.col("key2").expr :: Nil, numPartitions = 3)
    val partitioning2 = HashPartitioning(df1.col("key1").expr :: Nil, numPartitions = 3)

    val partitioning3 = HashPartitioning(df2.col("key1").expr :: Nil, numPartitions = 3)

    val distribution = ClusteredDistribution(Seq(df1.col("key1").expr, df1.col("key2").expr), requireAllClusterKeys = false)

    val satisfies1 = partitioning1.satisfies(distribution) // true，因为 partitioning1 完全匹配 distribution
    val satisfies2 = partitioning2.satisfies(distribution) // true，因为 partitioning2 匹配了部分键
    val satisfies3 = partitioning3.satisfies(distribution) // false，因为 df1 与 df2 数据源不一样

    println(s"Satisfies1: $satisfies1") // 输出：Satisfies1: true
    println(s"Satisfies2: $satisfies2") // 输出：Satisfies2: true
    println(s"Satisfies2: $satisfies3") // 输出：Satisfies2: false
  }

  test("group by") {
    //    sql("set spark.sql.codegen.wholeStage=false") // disable code gen HashAggregateExec.doExecute -> TungstenAggregationIterator
    //    sql("select category, sum(sales) from sales_table group by category").explain(true)
    sql("select category, sum(sales) from sales_table group by category").show(true)
  }

  test("window") {
    val df = sql(
      """
        |select
        | category, dt, sales
        | ,sum(sales) over(partition by category order by dt ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as t1
        | ,avg(sales) over(partition by dt order by category,sales ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as t2
        | ,row_number() over(partition by dt order by category,sales ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as t3
        |from sales_table
        |""".stripMargin)
    df.explain(true)
    df.show(true)
  }

  test("cube") {
    var df = sql(
      """
        |select
        | category, dt, sum(sales) as t
        |from sales_table
        |group by category, dt
        |with rollup
        |""".stripMargin)
    df.show(true)
    sql("set spark.sql.codegen.wholeStage=false")
    sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    df = sql(
      """
        |select
        | coalesce(a.category, b.category) as category,
        | coalesce(a.dt, b.dt) as dt,
        | a.sales,
        | b.counts
        |from sales_table a
        |full join counts_table b
        |on a.category = b.category and a.dt = b.dt
        |""".stripMargin)
    df.show(true)
  }

  test("cube & rollup") {
    val df = sql(
      """
        |select
        | dt, category as c1, c2, sum(sales) as total_sales
        |from sales_table
        |group by rollup(dt, category), c2
        |order by dt, c1, c2
        |""".stripMargin)
    df.show(true)
  }

  test("Dynamic Partition Pruning(DPP)") {
    // sql("set spark.sql.adaptive.enabled=true")
    sql("set spark.sql.codegen.wholeStage=false")
    val result = spark.sql(
      """
      SELECT f.id, f.value, d.event
      FROM fact_table f
      JOIN dim_table d
      ON f.date = d.date
      WHERE d.event = 'New Year'
    """)
    result.show()
     Thread.sleep(1000 * 3600)
  }

  test("codegen") {
    //sql("set spark.sql.codegen.wholeStage=false") // disable code gen
    val df = sql("select id,category from sales_table where sales > 100")
    df.show(true)
    //    sql("drop table if exists sales_table2")
    //    sql(
    //      """
    //        |create table if not exists sales_table2 as
    //        | select id,category from sales_table where sales > 100
    //        |""".stripMargin)
    //    df.explain(true)
  }

  test("AdaptiveSparkPlanExec CoalesceShufflePartitions1") {
    // sql("set spark.sql.adaptive.enabled=false")
    sql("drop table if exists t1")
    sql("drop table if exists t2")
    sql("drop table if exists t3")

    val df1 = spark.range(0, 1000000).select($"id".as("key"), rand().as("value1")).repartition($"key")
    df1.createOrReplaceTempView("t1")

    val df2 = spark.range(0, 1000000).select($"id".as("key"), rand().as("value2")).repartition($"key")
    df2.createOrReplaceTempView("t2")
    sql(
      """
        |select
        | t1.key,
        | t1.value1,
        | t2.value2
        |from t1 join t2 on t1.key = t2.key
        |where t2.value2 < 0.001
        |""".stripMargin).write.mode(SaveMode.Overwrite).saveAsTable("t3")
    //    import org.apache.spark.sql.functions.broadcast
    //    val joined = df1.join(broadcast(df2), "key")
    //    joined.show()
    Thread.sleep(1000 * 3600)
  }

  test("AdaptiveSparkPlanExec CoalesceShufflePartitions2") {
    val data = (1 to 1000).map(i => (i, i % 5))
    val df = spark.sparkContext.parallelize(data).toDF("id", "category")
    df.createOrReplaceTempView("t")

    sql(
      """
        |select sum(id) as sum_ids
        |from t
        |group by category
        |""".stripMargin).show()
    Thread.sleep(3600 * 1000L)
  }

  test("AdaptiveSparkPlanExec CoalesceShufflePartitions & OptimizeShuffleWithLocalRead") {
    sql("drop table if exists t1")
    sql("drop table if exists t2")
    sql("drop table if exists t3")

    val df1 = spark.range(0, 1000000).select($"id".as("key"), rand().as("value1")).repartition($"key")
    df1.createOrReplaceTempView("t1")

    val df2 = spark.range(0, 1000000).select($"id".as("key"), rand().as("value2"))
    df2.createOrReplaceTempView("t2")
    sql(
      """
        |select
        | t1.key,
        | t1.value1,
        | t2.value2
        |from t1 join t2 on t1.key = t2.key
        |where t2.value2 < 0.001
        |""".stripMargin).write.mode(SaveMode.Overwrite).saveAsTable("t3")

    Thread.sleep(1000 * 3600)
  }

}
// scalastyle:on