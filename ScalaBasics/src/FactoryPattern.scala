

object FactoryPattern extends App {

  trait Computer{
    
    def ram:String
    def hdd:String
    def cpu:String
    
  }
  // PC class extends computer class
  case class PC(ram:String,hdd:String,cpu:String) extends Computer
  case class LAPTOP(ram:String,hdd:String,cpu:String) extends Computer
  
  // we will hide instance creation logic inside ComputerFactory
  object ComputerFactory{
    def apply(compType:String,ram:String,hdd:String,cpu:String)  = compType match{
      case "PC" => PC(ram, hdd, cpu)
      case "LAPTOP" => LAPTOP(ram, hdd, cpu)
    }
   
    // here i want to get a instance of a PC , using ComputerFactory, 
    //and i don't have access to know how object is getting created
    ComputerFactory("PC","16gb","1tb","2.3gz")
    ComputerFactory("LAPTOP","16gb","1tb","2.3gz")
  }
}