package com.mocha.spark.rddtest

/**
  * 二次排序构造key
  *
  * @author Yangxq
  * @version 2017/7/9 0:45
  */
class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(other: SecondarySortKey): Int = {
    if (this.first - other.first != 0) {
      this.first - other.first
    } else {
      this.second - other.second
    }
  }
}
