// scalastyle:off
package cn.juntaozhang.example.spark

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SortMergeJoinSpec extends AnyFunSuite with BeforeAndAfterAll {

  val spark: SparkSession = SparkSession.builder()
    .appName("LogicalPlanTest")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  import spark._

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

  test("SortMergeJoin normal shuffle1") {
    sql("drop table if exists t1")
    sql("drop table if exists t2")

    sql(
      """
        |CREATE TABLE t1 (
        | id INT,
        | name STRING,
        | date STRING
        |) USING PARQUET
        |PARTITIONED BY (date)
        |""".stripMargin)

    sql(
      """
        |CREATE TABLE t2 (
        | id INT,
        | value INT,
        | date STRING
        |) USING PARQUET
        |PARTITIONED BY (date)
        |""".stripMargin)

    val t1Values = (1 to 20).map(i => s"($i, 'name$i', '2023-01-${"%02d".format(i % 5)}')").mkString(", ")
    sql(s"INSERT INTO t1 VALUES $t1Values")

    // t2: id is a multiple of 2
    val t2Values = (1 to 20).map(i => s"(${i * 2}, $i, '2023-01-${"%02d".format(i % 5)}')").mkString(", ")
    sql(s"INSERT INTO t2 VALUES $t2Values")
  }

  test("SortMergeJoin normal shuffle2") {
    val df = sql(
      """
        |SELECT t1.id, t1.name, t2.value
        |FROM t1
        |JOIN t2
        |ON t1.id = t2.id
        |""".stripMargin)
    df.show()

    //    sql("CREATE TEMPORARY VIEW c1 AS select * from t1 order by id")
    //    sql("CACHE TABLE c1")
    //    sql("select id, count(1) from c1 group by id order by id").show()
    //    sql("CREATE TEMPORARY VIEW c2 AS select * from t2 order by id")
    //    sql("CACHE TABLE c2")
    //    sql("select id, count(1) from c2 group by id order by id").show()
    //    val df = sql(
    //      """
    //        |SELECT /*+ MERGE(c1, c2) */ c1.id, c1.name, c2.value
    //        |FROM c1
    //        |JOIN c2
    //        |ON c1.id = c2.id
    //        |""".stripMargin)
    //    df.show()
    Thread.sleep(1000 * 3600)
  }

  /** *******************          BUCKET join         ************************* */

  test("BUCKET join remove shuffle1") {
    sql("drop table if exists t1")
    sql("drop table if exists t2")

    sql(
      """
        |CREATE TABLE t1 (
        | id INT,
        | name STRING,
        | date STRING
        |) USING PARQUET
        |PARTITIONED BY (date)
        |CLUSTERED BY (id) SORTED BY (id) INTO 3 BUCKETS
        |""".stripMargin)

    sql(
      """
        |CREATE TABLE t2 (
        | id INT,
        | value INT,
        | date STRING
        |) USING PARQUET
        |PARTITIONED BY (date)
        |CLUSTERED BY (id) SORTED BY (id) INTO 3 BUCKETS
        |""".stripMargin)

    val t1Values = (1 to 20).map(i => s"($i, 'name$i', '2023-01-${"%02d".format(i % 2)}')").mkString(", ")
    sql(s"INSERT INTO t1 VALUES $t1Values")

    // t2: id is a multiple of 2
    val t2Values = (1 to 20).map(i => s"(${i * 2}, $i, '2023-01-${"%02d".format(i % 2)}')").mkString(", ")
    sql(s"INSERT INTO t2 VALUES $t2Values")
  }

  test("SortMergeJoin run") {
//    sql("SET spark.sql.adaptive.enabled = false")
//    sql("SET spark.sql.shuffle.partitions = 2")
    val df = sql(
      """
        |SELECT t1.id, t1.name, t2.value
        |FROM t1
        |JOIN t2
        |ON t1.id = t2.id
        |where t2.value > 1
        |""".stripMargin)
    println("===================== df.show() =============================")
    df.show()
    //Thread.sleep(1000 * 3600)
  }

  test("optimizeSkewJoin - prepare") {
    spark
      .range(0, 1000, 1, 10)
      .selectExpr("id % 3 as key1", "id as value1")
      .write.mode(SaveMode.Overwrite).saveAsTable("skewData1")
    spark
      .range(0, 1000, 1, 10)
      .selectExpr("id % 1 as key2", "id as value2")
      .write.mode(SaveMode.Overwrite).saveAsTable("skewData2")
  }

  test("optimizeSkewJoin") {
    sql("drop table if exists skewDataJoin")
    sql("drop table if exists skewData1")
    sql("drop table if exists skewData2")

    spark
      .range(0, 1000, 1, 10)
      .selectExpr("id % 3 as key1", "id as value1")
      .createOrReplaceTempView("skewData1")
    spark
      .range(0, 1000, 1, 10)
      .selectExpr("id % 1 as key2", "id as value2")
      .createOrReplaceTempView("skewData2")

    sql("SET spark.sql.adaptive.enabled = true")
    sql("SET spark.sql.autoBroadcastJoinThreshold = -1")
    sql("SET spark.sql.adaptive.optimize.skewsInRebalancePartitions.enabled = false")
    sql("SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 2k")
    sql("SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 2k")      //
    sql("SET spark.sql.adaptive.coalescePartitions.minPartitionNum = 1") // 最小分区数
    sql("SET spark.sql.shuffle.partitions = 10")                         // shuffle 分区数
    sql("SET spark.sql.adaptive.forceOptimizeSkewedJoin = true")

//    sql("SET spark.sql.files.openCostInBytes = 0")
//    sql("SET spark.sql.files.maxPartitionBytes = 1300")

    println("===============================================================")
    sql(
      """
        |SELECT key1
        |      ,value1
        |      ,key2
        |      ,value2
        |FROM skewData1
        |JOIN skewData2 ON key1 = key2
        |""".stripMargin).write.mode(SaveMode.Overwrite).saveAsTable("skewDataJoin")

//    sql(
//      """
//        |SELECT /*+ repartition(3, key1) */ key1, count(1)
//        |FROM (skewData1 JOIN skewData2 ON key1 = key2)
//        |GROUP BY key1
//        |""".stripMargin).write.mode(SaveMode.Overwrite).saveAsTable("skewDataJoin")

//    sql("select key1, count(1) from skewData1 group by key1").show()
//    sql("select key2, count(1) from skewData2 group by key2").show()
//    sql("select key1, count(1) from skewDataJoin group by key1").show()
    Thread.sleep(1000 * 3600)
  }

}
// scalastyle:on