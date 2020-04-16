package rabbitmq.publishsubscribe

import com.rabbitmq.client.{CancelCallback, ConnectionFactory, DeliverCallback}

object ReceiveLogs {

  private val EXCHANGE_NAME="logs"

  def main(args: Array[String]): Unit = {
    val factory=new ConnectionFactory
    factory.setUsername("hashini")
    factory.setPassword("1995")
    factory.setHost("localhost")
    val connection= factory.newConnection()
    val channel= connection.createChannel()
    channel.exchangeDeclare(EXCHANGE_NAME,"fanout")
    val queueName1=channel.queueDeclare().getQueue
    channel.queueBind(queueName1,EXCHANGE_NAME,"")
    val queueName2=channel.queueDeclare().getQueue
    channel.queueBind(queueName2,EXCHANGE_NAME,"")
    println(" [*] Waiting for messages. To exit press CTRL+C")
    val deliverCallback:DeliverCallback= (_,delivery) =>{
      val message= new String(delivery.getBody,"UTF-8")
      println(" [x] Received '" + message + "'")
    }
    val cancel:CancelCallback = _ => {}
    channel.basicConsume(queueName1,true,deliverCallback,cancel)
    channel.basicConsume(queueName2,true,deliverCallback,cancel)
  }
}
