package com.icct.ais.sparkbootcamp

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Nothing to see here.  This exercise is actually in part 3" )
    println("concat arguments = " + foo(args))
  }

}
