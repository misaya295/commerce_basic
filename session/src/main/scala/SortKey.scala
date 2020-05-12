case class SortKey(clickCout:Long,orderCount:Long,payCount:Long) extends Ordered[SortKey]{

  //this.compare(that)
  //this.compare that
  //compare > 0 this > that
  //compare < 0 this < that
  override def compare(that: SortKey): Int = {

    if (this.clickCout - that.clickCout != 0) {


      return (this.clickCout - that.clickCout).toInt
    }else if (this.orderCount -that.orderCount != 0) {
      return (this.orderCount - that.orderCount).toInt
    }else {
      return  (this.payCount - this.payCount).toInt
    }


  }
}
