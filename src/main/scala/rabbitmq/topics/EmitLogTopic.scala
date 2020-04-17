package rabbitmq.topics

import com.rabbitmq.client.ConnectionFactory

object EmitLogTopic {

  private val EXCHANGE_NAME="topic_logs"

  def main(args: Array[String]): Unit = {
    val factory = new ConnectionFactory
    factory.setUsername("hashini")
    factory.setPassword("1995")
    factory.setHost("localhost")
    val connection= factory.newConnection()
    val channel =connection.createChannel()
    channel.exchangeDeclare(EXCHANGE_NAME,"topic")
    val routingKey= getRouting(args)
    val message= getMessage(args)
    channel.basicPublish(EXCHANGE_NAME,routingKey,null,message.getBytes("UTF-8"))
    println(" [x] Sent '" + routingKey + "':'" + message + "'")
    channel.close()
    connection.close()
  }

  def getRouting(strings:Array[String]):String={
    if (strings.length<1) return "anonymous.info"
    strings(0)
  }

  def getMessage(strings:Array[String]):String={
    if(strings.length<2) return "Hello World"
    joinStrings(strings," ",1)
  }

  def joinStrings(strings: Array[String], delimiter:String, startIndex:Int):String={
    val length= strings.length
    if(length==0 || length<startIndex) return ""
    val words= new StringBuilder(strings(startIndex))
    for (i <- startIndex+1 until length){
      words.append(delimiter).append(strings(i))
    }
    words.toString()
  }
}
