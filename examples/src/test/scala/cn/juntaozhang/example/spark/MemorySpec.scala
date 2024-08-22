// scalastyle:off
package cn.juntaozhang.example.spark

import org.apache.spark.unsafe.{Platform, UnsafeAlignedOffset}
import org.apache.spark.unsafe.memory.{HeapMemoryAllocator, UnsafeMemoryAllocator}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class MemorySpec extends AnyFunSuite with BeforeAndAfterAll {
  test("HeapMemoryAllocator") {
    val allocator = new HeapMemoryAllocator()
    val block1 = allocator.allocate(1024)
    val offset = block1.getBaseOffset
    println("First array base offset: " + offset)
    Platform.putLong(block1.getBaseObject, offset, 123L)
    println(Platform.getLong(block1.getBaseObject, block1.getBaseOffset))

    Platform.putLong(block1.getBaseObject, offset, 321L)
    println(Platform.getLong(block1.getBaseObject, block1.getBaseOffset))

    val s = "hello spark!"
    val start = offset + 8
    for (i <- 0 until s.length) {
      Platform.putByte(block1.getBaseObject, start + i, s.charAt(i).toByte)
    }

    for (i <- 0 until s.length) {
      print(Platform.getByte(block1.getBaseObject, start + i).toChar)
    }

    println("\nfree memory")
    allocator.free(block1)
    //    try {
    //      Platform.putLong(block1.getBaseObject, offset, 321L)
    //    } catch {
    //      case e: Throwable => println(e)
    //    }
  }

  test("UnsafeMemoryAllocator") {
    val allocator = new UnsafeMemoryAllocator()
    val block1 = allocator.allocate(1024)
    val offset = block1.getBaseOffset
    println("base offset: " + offset)

    Platform.putLong(block1.getBaseObject, offset, 123L)
    println(Platform.getLong(block1.getBaseObject, block1.getBaseOffset))

    Platform.putLong(block1.getBaseObject, offset, 321L)
    println(Platform.getLong(block1.getBaseObject, block1.getBaseOffset))

    val start = offset + 8
    val s = "hello juntao!".getBytes()
    Platform.copyMemory(s, Platform.BYTE_ARRAY_OFFSET + 1, block1.getBaseObject, start, s.length - 1) // only compy from [1, s.length]

    // ello juntao!
    for (i <- 0 until s.length - 1) {
      print(Platform.getByte(block1.getBaseObject, start + i).toChar)
    }
    println()


    val uaoSize = UnsafeAlignedOffset.getUaoSize
    println("uao size: " + uaoSize)
    val actualSize = UnsafeAlignedOffset.getSize(block1.getBaseObject, offset)
    println("actual size: " + actualSize)

  }

  test("Aligned") {
    val a = UnsafeAlignedOffset.getUaoSize()
  }


}
// scalastyle:on
