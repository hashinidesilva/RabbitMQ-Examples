package rabbitmq.routing

import com.rabbitmq.client.{CancelCallback, ConnectionFactory, DeliverCallback}

object ReceiveLogsDirect {

  private val EXCHANGE_NAME ="direct_logs"

  def main(args: Array[String]): Unit = {
    val factory =new ConnectionFactory
    factory.setUsername("hashini")
    factory.setPassword("1995")
    factory.setHost("localhost")
    val connection = factory.newConnection()
    val channel = connection.createChannel()
    channel.exchangeDeclare(EXCHANGE_NAME,"direct")
    val queueName= channel.queueDeclare().getQueue
    if (args.length < 1) {
      System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]")
      System.exit(1)
    }
    for(severity <- args){
      channel.queueBind(queueName,EXCHANGE_NAME,severity)
    }
    println(" [*] Waiting for messages. To exit press CTRL+C")
    val deliverCallback: DeliverCallback = (_, delivery) => {
      val message = new String(delivery.getBody, "UTF-8")
      println(" [x] Received '" +
        delivery.getEnvelope.getRoutingKey + "':'" + message + "'")
    }
    val cancel:CancelCallback = _ => {}
    channel.basicConsume(queueName, true, deliverCallback, cancel)
  }
}
