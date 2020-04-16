package rabbitmq.helloworld

import com.rabbitmq.client._

object Consumer {

  private val QUEUE_NAME = "hello"

  def main(args: Array[String]) {
    val factory = new ConnectionFactory()
    factory.setHost("localhost")
    val connection = factory.newConnection()
    val channel = connection.createChannel()
    channel.queueDeclare(QUEUE_NAME, false, false, false, null)
    println(" [*] Waiting for messages. To exit press CTRL+C")
    val deliverCallback: DeliverCallback = (_, delivery) => {
      val message = new String(delivery.getBody, "UTF-8")
      println(" [x] Received '" + message + "'")
    }
    val cancel:CancelCallback= _ => {}
    channel.basicConsume(QUEUE_NAME,true,deliverCallback, cancel)
  }

}
