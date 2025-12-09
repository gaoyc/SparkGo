package com.kigo.bigdata.spark

// ScalaTest 3.0.x 风格（兼容 Scala 2.11 和 2.12）
import org.scalatest.{FlatSpec, Matchers}

class MySpec extends FlatSpec with Matchers {
  "This wonderful system" should "save the world" in {
    val list = Nil
    list shouldBe empty
  }

}

//// 新的 import 路径（ScalaTest 3.1+）
//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.matchers.should.Matchers
//
//class MySpec2 extends AnyFlatSpec with Matchers {
//  "This wonderful system" should "save the world" in {
//    val list = Nil
//    list shouldBe empty
//  }
//
//  // 注意：registerTest 和 registerIgnoredTest 是内部方法，通常不应 override！
//  // 如果你是为了动态注册测试用例，请改用其他方式（如 fixture 或 tags）
//  // 否则建议删除这两个 override 方法
//}