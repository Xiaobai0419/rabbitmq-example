package com.cn.simple;

import com.cn.ConnectionUtil;
import com.cn.exception.MessageFailException;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;

/**
 * @program: rabbit-learn
 * @description: 消费者
 * @author: 535504
 * @create: 2018-04-26 15:32
 **/
public class Consumer {

    private static final String QUEUE_NAME = "test_queue";

    public static void main(String[] args) throws IOException {
        Connection connection = ConnectionUtil.getConnection();
        final Channel channel = connection.createChannel(1);
        channel.queueDeclare(QUEUE_NAME,true,false,false,null);
        // 每次从队列获取的数量--可用于根据消费端处理能力调整消费速度，也可用于多个消费端配合消费同一个Queue,对同一个Queue全量消息的多个分流消费
        channel.basicQos(10);
        //自4.0+ 版本后无法再使用QueueingConsumer，而官方推荐使用DefaultConsumer
        com.rabbitmq.client.Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
                throws IOException {
                try {
                    Thread.sleep(2000);
                    super.handleDelivery(consumerTag, envelope, properties, body);
                    String message = new String(body,"UTF-8");
                    //模拟消费端消费（处理）失败，没能进行消息确认的场景
                    if("Hello World!0123".equalsIgnoreCase(message)) {
                        throw new MessageFailException("Message failed");
                    }
                    System.out.println(message);
                    channel.basicAck(envelope.getDeliveryTag(),false);//进行手动消息确认--没确认的消息驻留在内存，可以重复消费
                } catch (MessageFailException e) {
                    //重新消费一次，再失败则由死信队列处理；另一种方案是什么都不做，由专门的失败处理消费端对同一个Queue再次消费，失败了也要进入死信队列，防止循环消费不可能成功的消息
                    System.out.println(e.getMessage());
                    //经实验：如果这里什么都不做，需要手动断开这个消费失败（即未能进行消息确认）的消费者连接，Broker才会判定消息需要重入队列，这样其他消费同一个Queue的消费端才能对未确认消息进行消费，否则未确认消息一直驻留Broker无法被消费
                    //这里的处理：主动通知Broker把未能消费成功（即未能进行消息确认）的消息重新放回队列供再次消费，但这种处理会造成异常消费的消息始终异常，循环放回队列进行不可能成功的消费，严重影响业务运营
                    //所以这里正确的处理办法应该是让Broker将消息放入专门的死信队列，由专门的消费端线程统一处理
//                    channel.basicNack(envelope.getDeliveryTag(), false, true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        //监听队列，当b为true时，为自动提交（只要消息从队列中获取，无论消费者获取到消息后是否成功消息，都认为是消息已经成功消费），
        // 当b为false时，为手动提交（消费者从队列中获取消息后，服务器会将该消息标记为不可用状态，等待消费者的反馈，
        // 如果消费者一直没有反馈，那么该消息将一直处于不可用状态。
        //如果选用自动确认,在消费者拿走消息执行过程中出现宕机时,消息可能就会丢失！！）
        //使用channel.basicAck(envelope.getDeliveryTag(),false);进行消息确认
        channel.basicConsume(QUEUE_NAME,false,consumer);//第二个参数false：设置为手动确认
    }
}
