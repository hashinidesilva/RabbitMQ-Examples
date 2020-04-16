package rabbitmq.workqueues

import com.rabbitmq.client._

object Worker {
  private val TASK_QUEUE_NAME = "task_queue"

  def main(args: Array[String]) {
    val factory = new ConnectionFactory()
    factory.setHost("localhost")
    val connection = factory.newConnection()
    val channel = connection.createChannel()
    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null)
    println(" [*] Waiting for messages. To exit press CTRL+C")
    channel.basicQos(2)
    val deliverCallback: DeliverCallback = (_, delivery) => {
      val message = new String(delivery.getBody, "UTF-8")
      println(" [x] Received '" + message + "'")
      try {
        doWork(message)
      } finally {
        println(" Done")
        channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
      }
    }
    val cancel:CancelCallback= _ => {}
    channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, cancel)
  }

  private def doWork(task: String) {
    print(" [x] Processing ")

    for (ch <- task.toCharArray if ch == '.') {
      try {
        print(".")
        Thread.sleep(1000)
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }
  }
}
