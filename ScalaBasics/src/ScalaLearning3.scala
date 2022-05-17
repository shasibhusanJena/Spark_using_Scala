

object ScalaLearning3 {
  def main(args: Array[String]): Unit = {
    val a = Array(1,2,3,4,5)
    println(a.mkString( ","))
    for(i <- a)println(i)
    
    val b = List(1,2,3,4,5)
    println(b.head)
    println(b.tail)
    println(b.reverse)
    
    // Set
    val zx = Set(1,1,1,2,2,3,3,4)
    zx.min
    zx.max
    zx.sum
    
    val vx = Map(1 -> "Sumit",2 -> "Amit")
    print(vx.get(1))
  
  }
}