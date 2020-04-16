package rabbitmq.workqueues

import com.rabbitmq.client.{ConnectionFactory, MessageProperties}

object NewTask {

  private val TASK_QUEUE_NAME = "task_queue"

  def main(args: Array[String]): Unit = {
    val factory= new ConnectionFactory()
    factory.setHost("localhost")
    val connection=factory.newConnection()
    val channel=connection.createChannel()
    val message= "Hello World........................"
    channel.queueDeclare(TASK_QUEUE_NAME,true,false,false,null)
    channel.basicPublish("",TASK_QUEUE_NAME,MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes("UTF-8"))
    println(" [x] Sent '"+message+"'")
    channel.close()
    connection.close()
  }
}
