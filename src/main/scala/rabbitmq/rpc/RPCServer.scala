package rabbitmq.rpc

import java.util.concurrent.CountDownLatch
import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client._

class ServerCallback(channel: Channel,latch: CountDownLatch) extends DeliverCallback{

  override def handle(consumerTag:String,delivery:Delivery): Unit ={
    var response:String =null
    val replyProps= new BasicProperties.Builder()
      .correlationId(delivery.getProperties.getCorrelationId)
      .build()
    try{
      val message= new String(delivery.getBody,"UTF-8")
      val n=java.lang.Integer.parseInt(message)
      println(" [.] fib(" + message + ")")
      response= ""+Fibonacci.fib(n)
    }catch {
      case e:Exception=>
        println(" [.] "+e.toString)
        response=""
    }finally {
      channel.basicPublish("",delivery.getProperties.getReplyTo,replyProps,
        response.getBytes("UTF-8"))
      channel.basicAck(delivery.getEnvelope.getDeliveryTag,false)
      latch.countDown()
    }
  }
}
object Fibonacci{
  def fib(n:Int):Int={
    if(n==0) return 0
    if(n==1) return 1
    fib(n-1)+fib(n-2)
  }
}

object RPCServer{

  private val RPC_QUEUE_NAME = "rpc_queue"

  def main(args: Array[String]): Unit = {
    var connection:Connection = null
    var channel:Channel=null
    try {
      val factory= new ConnectionFactory
      connection=factory.newConnection()
      channel=connection.createChannel()
      channel.queueDeclare(RPC_QUEUE_NAME,false,false,false,null)
      channel.basicQos(1)
      val latch =new CountDownLatch(1)
      val serverCallback=new ServerCallback(channel,latch)
      val cancel:CancelCallback=_=> {}
      channel.basicConsume(RPC_QUEUE_NAME,false,serverCallback,cancel)
      println(" [x] Awaiting RPC requests")
      latch.await()
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      if(connection!=null){
        try{
          connection.close()
        }catch {
          case _:Exception=>
        }
      }
    }

  }
}
