import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * TODO
 * Created by zhuzhixian on 16-11-19.
 */
public class FanoutExchangeDemo {

    public static void main(String[] args) throws IOException, TimeoutException{
        sendMsg();
    }

    public static void sendMsg() throws IOException, TimeoutException {
        Channel channel = initSendChannel("fanoutKey");
        String message = "Hello World!";
        channel.basicPublish("", "fanoutKey", MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
        channel.close();
    }


    private final static String DIRECT_EXCHAGE = "helloFanoutExchange";

    private static Channel initSendChannel(String routeKey) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        channel.exchangeDeclare(DIRECT_EXCHAGE, "fanout", true);
        return channel;
    }

    private static Channel initConsumerChannel(String queue) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        channel.queueDeclare(queue, true, false, false, null);
        channel.basicQos(1);
        return channel;
    }

    private static Consumer generateConsumer(final Channel channel){
        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                channel.basicAck(envelope.getDeliveryTag(), false);
                System.out.println("body:"+new String(body)+", consumerTag:"+consumerTag);
            }

            @Override
            public void handleRecoverOk(String consumerTag) {
            }

            @Override
            public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
            }

            @Override
            public void handleCancel(String consumerTag) throws IOException {
            }

            @Override
            public void handleCancelOk(String consumerTag) {
            }

            @Override
            public void handleConsumeOk(String consumerTag) {
            }
        };
        return consumer;
    }
}
