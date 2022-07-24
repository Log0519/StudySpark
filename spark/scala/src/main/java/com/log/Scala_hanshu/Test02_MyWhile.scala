package com.log.Scala_hanshu

object Test02_MyWhile {
  def main(args: Array[String]): Unit = {
    var n=10

    //1.常规的while循环
    while (n >= 1){
      println(n)
      n=n-1
    }

    //2.用闭包实现一个函数,将代码块作为参数传入
    def myWhile(condition: =>Boolean):(=>Unit)=>Unit = {
      //内层函数需要递归调用,参数是循环体,
      def  doLoop(op: =>scala.Unit): Unit = {
        if(condition){
          op
          myWhile(condition)(op)
        }
      }
      doLoop _
    }
    n = 10;
    myWhile(n>=1){
      println(n)
      n-=1
    }

  }
}
