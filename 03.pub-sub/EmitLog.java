import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EmitLog {

  private static final String EXCHANGE_NAME = "logs";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");

    try (Connection connection = factory.newConnection();
        Channel channel = connection.createChannel()) {

      /*
       * The fanout exchange is very simple. As you can probably guess from the name, it just
       * broadcasts all the messages it receives to all the queues it knows. And that's exactly what
       * we need for our logger.
       * 
       * There are a few exchange types available: direct, topic, headers and fanout. We'll focus on
       * the last one -- the fanout. Let's create an exchange of this type, and call it logs:
       */
      channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

      String message = argv.length < 1 ? "info: Hello World!" : String.join(" ", argv);

      channel.basicPublish( EXCHANGE_NAME, 
                            "", 
                            null, 
                            message.getBytes("UTF-8"));
      
      System.out.println(" [x] Sent '" + message + "'");
    }
  }
}
