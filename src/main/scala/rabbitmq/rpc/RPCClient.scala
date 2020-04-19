package rabbitmq.rpc

import java.util.UUID
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._

class ResponseCallback(corrId:String) extends DeliverCallback {
  val response:BlockingQueue[String] = new ArrayBlockingQueue[String](1)
  override def handle(consumerTag: String, message: Delivery): Unit = {
    if(message.getProperties.getCorrelationId.equals(corrId)){
      response.offer(new String(message.getBody,"UTF-8"))
    }
  }

  def take():String={
    response.take()
  }
}

class RPCClient(host:String) {
  val factory= new ConnectionFactory
  factory.setUsername("hashini")
  factory.setPassword("1995")
  factory.setHost(host)
  val connection: Connection = factory.newConnection()
  val channel: Channel =connection.createChannel()
  val requestQueueName:String ="rpc_queue"
  val replyQueueName:String=channel.queueDeclare().getQueue

  def call(message:String): String ={
    val corrId= UUID.randomUUID().toString
    val props = new BasicProperties.Builder()
      .correlationId(corrId)
      .replyTo(replyQueueName)
      .build()
    channel.basicPublish("",requestQueueName,props,message.getBytes("UTF-8"))
    val responseCallback = new ResponseCallback(corrId)
    val cancel:CancelCallback = _=> {}
    channel.basicConsume(replyQueueName,true,responseCallback,cancel)
    responseCallback.take()
  }

  def close(): Unit ={
    connection.close()
  }
}

object RPCClient{

  def main(args: Array[String]): Unit = {
    var fibonacciRpc:RPCClient=null
    var response: String = null
    try {
      val host = if(args.isEmpty) "localhost" else args(0)
      fibonacciRpc =new RPCClient(host)
      println(" [x] Requesting fib(30)")
      response=fibonacciRpc.call("2")
      println(" [.] Got '" + response + "'")
    }catch {
      case e:Exception => e.printStackTrace()
    }
    finally {
      if(fibonacciRpc!=null){
        try {
          fibonacciRpc.close()
        }catch {
          case _:Exception =>
        }
      }
    }
  }
}
