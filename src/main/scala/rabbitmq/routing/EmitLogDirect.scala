package rabbitmq.routing

import com.rabbitmq.client.ConnectionFactory

object EmitLogDirect {

  private val EXCHANGE_NAME = "direct_logs"

  def main(args: Array[String]): Unit = {
    val factory =new ConnectionFactory
    factory.setUsername("hashini")
    factory.setPassword("1995")
    factory.setHost("localhost")
    val connection = factory.newConnection()
    val channel = connection.createChannel()
    channel.exchangeDeclare(EXCHANGE_NAME,"direct")
    val severity =getSeverity(args)
    val message =getMessage(args)
    channel.basicPublish(EXCHANGE_NAME,severity,null,message.getBytes("UTF-8"))
    println(" [x] Sent '" + severity + "':'" + message + "'")
    channel.close()
    connection.close()
  }

  private def getSeverity(strings:Array[String]):String={
    if(strings.length <1) return "info"
    strings(0)
  }

  private def getMessage(strings:Array[String]):String={
    if (strings.length<2) return "Hello world"
    joinStrings(strings)
  }

  private def joinStrings(strings: Array[String]):String={
    val length=strings.length
    if(length==0 || length < 1) return ""
    val words= new StringBuilder(strings(1))
    for (i <- 2 until length){
      words.append(" ").append(strings(i))
    }
    words.toString()
  }
}
