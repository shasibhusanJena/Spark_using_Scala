
// for anf forEach loop both give the same output, 
// but scala internally converts For loop to forEach loop
package org.fundamental.one

object ForEach extends App {
  for(i <- 1 to 10) println(i*i)
  
  (1 to 10).foreach(i => println(i*i) )
  
  for(j <- 1 to 10) yield print(j*j+" ")
  
  (1 to 10).map(i => print(i*i+" "))  
  println("")
  
  for(i <- 1 to 10; j<- 'a' to 'c') yield print(i*2 + ""+j+" ")
  
  // in java == is a operator where as in scala there is no operator , everything is a method or function
  // == is inernally implemented as equals operator in scala
  var a = new String("shasi")
  var b = new String("shasi")
  if(a ==b){
    
    print("\ntrue")
  }
  
}