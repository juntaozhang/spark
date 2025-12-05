// scalastyle:off
package cn.juntaozhang.example.spark

import org.apache.spark.sql.SparkSession
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

  /*********************          BUCKET join         **************************/

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
    sql("set spark.sql.adaptive.enabled=false")
    val df = sql(
      """
        |SELECT t1.id, t1.name, t2.value
        |FROM t1
        |JOIN t2
        |ON t1.id = t2.id
        |""".stripMargin)
    println("===================== df.show() =============================")
    df.show()
    // Thread.sleep(1000 * 3600)
  }

}
// scalastyle:on