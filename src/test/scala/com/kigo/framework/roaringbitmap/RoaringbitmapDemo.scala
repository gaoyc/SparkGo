package com.kigo.framework.roaringbitmap

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.ByteBuffer

import org.junit.Test
import org.roaringbitmap.RoaringBitmap
import org.roaringbitmap.buffer.{ImmutableRoaringBitmap, MutableRoaringBitmap}

/**
  * RoaringBitmap基本使用示例<p>
  * 官网参考: <https://github.com/lemire/RoaringBitmap>
  * Created by Kigo Gao on 2020/3/6.
  */
class RoaringbitmapDemo {

  @Test
  def testBasic(): Unit = {

    //生成一个bitmap, 根据指定的数字设置为true
    val rr = RoaringBitmap.bitmapOf(1, 2, 3, 1000)
    val rr2 = new RoaringBitmap()
    // 向rr2增加
    for (i <- (4000 to 4255)) {
      rr2.add(i)
    }
    // rr2.add(4000, 4255) //高版本可直接按范围设置

    // 返回指定位的数字
    println("rr.select(3) :"+ rr.select(3))

    // 返回小于或者等于指定值的有效位(设置为1)的元素数字
    println("rr.rank(998): "+ rr.rank(999))

    //判断是否包含1000
    rr.contains(1000); // will return true
    //判断是否包含7
    rr.contains(7); // will return false

    //两个RoaringBitmap进行or操作，数值进行合并，合并后产生新的RoaringBitmap叫rror
    val rror = RoaringBitmap.or(rr, rr2);
    //rr与rr2进行位运算，并将值赋值给rr
    rr.or(rr2);
    //判断rror与rr是否相等，显然是相等的
    val equals = rror.equals(rr);

    // 查看rr中存储了多少个值，1,2,3,1000和10000-12000，共2004个数字
    val cardinality = rr.getCardinality
    println("cardinality: "+cardinality);



    //遍历rr中的value, 即获取所有设置为1的数字
    val iter = rr.iterator()
    while (iter.hasNext){
      val num = iter.next()
      println("iter: "+ num)
    }

    // 如果要使用基于memory-maped files的bitmaps，可以使用org.roaringbitmap.buffer包来代替

  }

  /**
    * The following code sample illustrates how to create an ImmutableRoaringBitmap from a ByteBuffer.
    * In such instances, the constructor only loads the meta-data in RAM while the actual data is accessed
    * from the ByteBuffer on demand.
    */
  def testImmutableRoaringBitmap(): Unit = {

    val rr1 = MutableRoaringBitmap.bitmapOf(1, 2, 3, 1000);
    val rr2 = MutableRoaringBitmap.bitmapOf( 2, 3, 1010);
    val bos = new ByteArrayOutputStream();
    val dos = new DataOutputStream(bos);
    rr1.serialize(dos);
    rr2.serialize(dos);
    dos.close();
    val bb = ByteBuffer.wrap(bos.toByteArray());
    val rrback1 = new ImmutableRoaringBitmap(bb);
    bb.position(bb.position() + rrback1.serializedSizeInBytes());
    val rrback2 = new ImmutableRoaringBitmap(bb);

//    Operations on an ImmutableRoaringBitmap such as and, or, xor, flip, will
    // generate a RoaringBitmap which lies in RAM. As the name suggest, the
    // ImmutableRoaringBitmap itself cannot be modified.

  }


}
