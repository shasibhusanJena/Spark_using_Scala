/*The first line of Input consists of two space separated integer size and numK, representing the number of elements in the list(N) .and the integer to be compared (K). The second line consists of N space separated integer - elements[0], elements[1] .. elements[N-1] representing the list of integer
*/
// 5 20
// 10 20 30 40 50 
package org.fundamental.one

import scala.io.StdIn._
object checkLessorValues {
  def main(args: Array[String]): Unit = {
  val inputLine1 = readLine()
  val inputLine1Arr = inputLine1.split(" ").map(_.toInt)
  val inputLine2 = readLine()
  val inputLine2Arr = inputLine2.split(" ").map(_.toInt)
  val numK = inputLine1Arr(1)
  var count = 0
  for(i <- inputLine2Arr){
    if(i <=numK){
      count =count+1
    }
  }
  print(count)
  }
}