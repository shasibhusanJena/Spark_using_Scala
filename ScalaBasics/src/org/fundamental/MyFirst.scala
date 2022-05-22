package org.fundamental

object MyFirst extends App {
  
  println("Hello world")
  def add(num: Int=8,incrementBy: Int =1)= {
    num + incrementBy
  }
  
  println(add(7))

  case class Address(city:String,State: String)
  
  class User(email: String,password:String){
    var firstName: Option[String] = None
    var lastName: Option[String] = None
    var address: Option[Address] = None
  
  }
  
  val user = new User("Shasibusan","Jena" )
  println(user.firstName.getOrElse("<not asigned>"))

  user.firstName = Some("Shasi")
  user.lastName = Some("J")
  user.address = Some(Address("Blore", "Karnataka"))

}