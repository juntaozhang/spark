// scalastyle:off
package org.apache.spark.sql

import org.apache.spark.sql.execution.adaptive.ShufflePartitionsUtil
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SqlSpec extends AnyFunSuite with BeforeAndAfterAll {
  test("splitSizeListByTargetSize") {
    // 第一块比较大无法拆分
    val sizes = Array(256L, 20L, 100L, 10L, 30L)
    val targetSize = 128L
    val smallPartitionFactor = 0.2
    val partitionStartIndices = ShufflePartitionsUtil.splitSizeListByTargetSize(sizes, targetSize, smallPartitionFactor)
    println(partitionStartIndices.mkString(", "))
  }

  test("splitSizeListByTargetSize-2") {
    // 50L 30L, 30L 无法合并到一起
    val sizes = Array(128L, 50L, 128L, 30L, 30L)
    val targetSize = 128L
    val smallPartitionFactor = 0.2
    val partitionStartIndices = ShufflePartitionsUtil.splitSizeListByTargetSize(sizes, targetSize, smallPartitionFactor)
    println(partitionStartIndices.mkString(", "))
  }

  def equallyDivide(numElements: Int, numBuckets: Int): Seq[Int] = {
    val elementsPerBucket = numElements / numBuckets
    val remaining = numElements % numBuckets
    val splitPoint = (elementsPerBucket + 1) * remaining
    (0 until remaining).map(_ * (elementsPerBucket + 1)) ++
      (remaining until numBuckets).map(i => splitPoint + (i - remaining) * elementsPerBucket)
  }

  test("equallyDivide") {
    println(equallyDivide(10, 3))
  }


  test("future") {
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.Future
    println("===begin===")
    val f = Future {
      println("Future-start")
      Thread.sleep(1000)
      println("Future-end")
      1
    }
    f.onComplete { x =>
      println(x)
    }
    println("===end===")
    Thread.sleep(2000)
    println("end")
  }

}
// scalastyle:on