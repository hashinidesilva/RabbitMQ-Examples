package rabbitmq.topics

import com.rabbitmq.client.{CancelCallback, ConnectionFactory, DeliverCallback}

object ReceiveLogsTopic {

  private val EXCHANGE_NAME="topic_logs"

  def main(args: Array[String]): Unit = {
    val factory =new ConnectionFactory
    factory.setUsername("hashini")
    factory.setPassword("1995")
    factory.setHost("localhost")
    val connection=factory.newConnection()
    val channel = connection.createChannel()
    channel.exchangeDeclare(EXCHANGE_NAME,"topic")
    val queueName= channel.queueDeclare().getQueue
    if(args.length<1){
      System.err.println("Usage: ReceiveLogsTopic [binding_key]...")
      System.exit(1)
    }
    for(bindingKey <- args){
      channel.queueBind(queueName,EXCHANGE_NAME,bindingKey)
    }
    println(" [*] Waiting for messages. To exit press CTRL+C")
    val deliveryCallback:DeliverCallback = (_,delivery)=>{
      val message= new String(delivery.getBody,"UTF-8")
      println(" [x] Received '" + delivery.getEnvelope.getRoutingKey + "':'" + message + "'")
    }
    val cancel:CancelCallback= _=>{}
    channel.basicConsume(queueName,true,deliveryCallback,cancel)
  }
}
