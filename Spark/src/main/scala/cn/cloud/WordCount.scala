package cn.cloud

/**
  * Description: 
  *
  *          Date: 2019-08-01
  *          Time: 13:31
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    var name = "cloud"
    println(s"my name is $name")
    println(sum(1, 2))
    sayHello
  }

  def sum(x: Int, y: Int) = {
    x + y
  }

  def sayHello = println("hello")
}
