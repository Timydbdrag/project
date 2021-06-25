package util

import scala.collection.mutable.ArrayBuffer

class RsiCalc(private var count:Int = 0) extends Serializable {

  private val list = new ArrayBuffer[Float]()
  private var prevPeriod:Float = 0f
  private var emaDown:Float = 0f
  private var emaUp:Float = 0f

  def calculation(closePrice:Float): Float = {
    var res = 50f
    if(count > 13) {
      res = nextCalc(closePrice)
    }
    else {
      list.append(closePrice)
      count+=1
      if(list.size == 14) {
        res = firstCalc()
      }
    }
    prevPeriod = closePrice

    println(count + "  >> " + res + " >> " + list.size)
    res
  }

  def firstCalc(): Float ={
    var res = 0f
    var tempEmaUp = 0f
    var tempEmaDown = 0f

    if (list.size >= 14) {
      for (i <- 1 until list.size) {

        if (list(i) > list(i-1))
          tempEmaUp += list(i) - list(i-1)
        else tempEmaDown += list(i-1) - list(i)
      }
      emaUp = tempEmaUp
      emaDown = tempEmaDown

      val RS = (tempEmaUp / 14) / (tempEmaDown / 14)
      res = 100 - (100 / (1 + RS))
    }

    res
  }

  def nextCalc(closePrice:Float): Float = {
    var tempUp:Float = 0f
    var tempDown:Float= 0f
    if (closePrice > prevPeriod)
      tempUp = closePrice - prevPeriod
    else tempDown = prevPeriod - closePrice

    val tempEmaUp:Float = ((emaUp * 13) + tempUp) / 14
    val tempEmaDown:Float = ((emaDown * 13) + tempDown) / 14

    val SmoothedRS = tempEmaUp / tempEmaDown
    val response = 100 - (100 / (1 + SmoothedRS))

    emaUp = tempEmaUp //перезаписываем значение Up
    emaDown = tempEmaDown //перезаписываем значение Down

    response
  }

}
