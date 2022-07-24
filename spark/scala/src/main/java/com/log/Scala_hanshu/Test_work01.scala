package com.log.Scala_hanshu

object Test_work01 {
  def main(args: Array[String]): Unit = {
//    def func(i:Int):String=>(Char=>Boolean)={
//      def f1(s:String):Char=>Boolean ={
//        def f2(c:Char):Boolean={
//          if(i==0&&s==""&&c=='0')false else true
//        }
//        f2
//      }
//      f1
//    }
//    println(func(0)("")('0'))
//    println(func(0)("")('1'))
def func(i:Int):String=>(Char=>Boolean)={
  s=>c=>if(i==0&&s==""&&c=='0')false else true
}
    println(func(0)("")('0'))
    println(func(0)("")('1'))

  }
}
