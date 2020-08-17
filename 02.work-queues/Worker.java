import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Worker {

  private static final String TASK_QUEUE_NAME = "task_queue";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");

    final Connection connection = factory.newConnection();
    final Channel channel = connection.createChannel();

    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    // Fair dispatch
    int prefetchCount = 1;
    channel.basicQos(prefetchCount);
    /*
     * In order to defeat that we can use the basicQos method with the prefetchCount
     * = 1 setting. This tells RabbitMQ not to give more than one message to a
     * worker at a time. Or, in other words, don't dispatch a new message to a
     * worker until it has processed and acknowledged the previous one. Instead, it
     * will dispatch it to the next worker that is not still busy.
     */

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), "UTF-8");

      System.out.println(" [x] Received '" + message + "'");
      try {
        doWork(message);
      } finally {
        System.out.println(" [x] Done");
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      }
    };

    /*
     * Manual message acknowledgments are turned on by default. In previous examples
     * we explicitly turned them off via the autoAck=true flag. It's time to set
     * this flag to false and send a proper acknowledgment from the worker, once
     * we're done with a task.
     */
    boolean autoAck = true; // acknowledgment is covered below
    channel.basicConsume(TASK_QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {
    });
  }

  private static void doWork(String task) {
    for (char ch : task.toCharArray()) {
      if (ch == '.') {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException _ignored) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}