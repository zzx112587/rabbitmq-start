import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by zhuzhixian on 16-11-15.
 */
public class DemoStarter {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {
        //sendMsg();
        receiveMsg();
    }

    public static void sendMsg() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        String message = "Hello World!";
        channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
        channel.close();
        connection.close();
    }

    public static void receiveMsg() throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        /*
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(QUEUE_NAME, true, consumer);
        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());
            System.out.println(" [x] Received '" + message + "'");
        }
        */

        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println(1);
            }

            @Override
            public void handleRecoverOk(String consumerTag) {
                System.out.println(2);
            }

            @Override
            public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                System.out.println(3);
            }

            @Override
            public void handleCancel(String consumerTag) throws IOException {
                System.out.println(4);
            }

            @Override
            public void handleCancelOk(String consumerTag) {
                System.out.println(5);
            }

            @Override
            public void handleConsumeOk(String consumerTag) {
                System.out.println(6);
                try {
                    channel.basicRecover(true);
                } catch (IOException e) {
                }
            }
        };
        channel.basicConsume(QUEUE_NAME, false, consumer);
    }
}
