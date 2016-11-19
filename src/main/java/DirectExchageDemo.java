import com.rabbitmq.client.*;
import com.sun.org.apache.xml.internal.security.Init;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * test send msg to direct exchange
 * Created by zhuzhixian on 16-11-19.
 */
public class DirectExchageDemo {

    public static void main(String[] args) throws InterruptedException, TimeoutException, IOException {

        //for(int i=0;i<100;i++){
            sendMsg();
        //}

        BlockingQueue<Runnable> threadQueue = new LinkedBlockingDeque<Runnable>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 100, 10L,TimeUnit.MINUTES, threadQueue);
        executor.submit(new Runnable() {
            public void run() {
                try {
                    Channel channel = initChannel(routeKey1, QUEUE_NAME);
                    channel.basicConsume(QUEUE_NAME, false, generateConsumer(channel, routeKey1));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        executor.submit(new Runnable() {
            public void run() {
                try {
                    Channel channel = initChannel(routeKey1, QUEUE_NAME);
                    channel.basicConsume(QUEUE_NAME, false, generateConsumer(channel, routeKey1));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        executor.submit(new Runnable() {
            public void run() {
                try {
                    Channel channel = initChannel(routeKey2, "hello2Direct");
                    channel.basicConsume("hello2Direct", false, generateConsumer(channel, routeKey2));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        Thread.sleep(3*1000);
        //sendMsg();
    }

    private final static String QUEUE_NAME = "helloDirect";
    private final static String DIRECT_EXCHAGE = "helloDirectExchange";

    private final static String routeKey1 = "green";
    private final static String routeKey2 = "red";

    public static void sendMsg() throws IOException, TimeoutException {
        Channel channel = initChannel(routeKey1, QUEUE_NAME);
        String message = "Hello World!";
        channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
        channel.close();
    }

    private static Channel initChannel(String routeKey, String queue) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        channel.exchangeDeclare(DIRECT_EXCHAGE, "direct", true);
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, DIRECT_EXCHAGE, routeKey);
        channel.basicQos(1);
        return channel;
    }

    private static Consumer generateConsumer(final Channel channel, final String routeKey){
        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("routeKey:"+routeKey+1+", thread:"+Thread.currentThread().getName());
                channel.basicAck(envelope.getDeliveryTag(), false);
                System.out.println("routeKey:"+new String(body)+", consumerTag:"+consumerTag);
            }

            @Override
            public void handleRecoverOk(String consumerTag) {
                System.out.println("routeKey:"+routeKey+2);
            }

            @Override
            public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                System.out.println(3);
            }

            @Override
            public void handleCancel(String consumerTag) throws IOException {
                System.out.println("routeKey:"+routeKey+4);
            }

            @Override
            public void handleCancelOk(String consumerTag) {
                System.out.println("routeKey:"+routeKey+5);
            }

            @Override
            public void handleConsumeOk(String consumerTag) {
                System.out.println("routeKey:"+routeKey+6+", thread:"+Thread.currentThread().getName());
                System.out.println("consumerTag:"+consumerTag+", routeKey:"+routeKey+", thread:"+Thread.currentThread().getName());
            }
        };
        return consumer;
    }

}
