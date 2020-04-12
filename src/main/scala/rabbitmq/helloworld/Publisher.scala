package rabbitmq.helloworld

import com.rabbitmq.client.ConnectionFactory

object Publisher {

  private val QUEUE_NAME = "hello"

  def main(args: Array[String]) {
    val factory = new ConnectionFactory()
    factory.setUsername("hashini")
    factory.setPassword("1995")
    val connection = factory.newConnection()
    val channel = connection.createChannel()
    //    channel.queueDelete("helloW")
    channel.queueDeclare(QUEUE_NAME, false, false, false, null)
    val message = "Hello World!"
    channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"))
    println(" [x] Sent '" + message + "'")
    channel.close()
    connection.close()
  }
}
