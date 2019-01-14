package com.cn.simple;

import com.cn.ConnectionUtil;
import com.cn.exception.MessageFailException;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @program: rabbit-learn
 * @description: 消费者
 * @author: 535504
 * @create: 2018-04-26 15:32
 **/
public class Consumer {

    //模拟从数据库或缓存中查询新创建的Queue，管理者实时根据吞吐量需要向数据库或缓存添加新的Exchange,Queue名称
    private static String[] new_queues = {"test_queue1","test_queue2","test_queue3"};

    public static void main(String[] args) throws IOException {
        final CountDownLatch latch = new CountDownLatch(60);//暂时这样设置：总共接收60条消息
        List<com.rabbitmq.client.Consumer> consumerList = new ArrayList<>();
        //创建一个新连接
        Connection connection = ConnectionUtil.getConnection();
        for(final String queue : new_queues) {
            //Channel是线程不安全的，每个消费者线程需要独占一个Channel进行队列消费
            final Channel channel1 = connection.createChannel();//这里不能设置参数为1，这样就限制了该Connection只能创建一个Channel,再试图创建均返回null,造成空指针异常
            System.out.println(channel1);
            final Channel channel2 = connection.createChannel();
            System.out.println(channel2);
            //消费端不负责创建queue,由发送端统一控制创建（从数据库动态查询）--暂定，具体谁创建看业务需要，甚至可事先或随时通过控制台创建好Exchange,Queue和routingKey绑定
//        channel.queueDeclare(QUEUE_NAME,true,false,false,null);
            // 每次从队列获取的数量--可用于根据消费端处理能力调整消费速度，也可用于多个消费端配合消费同一个Queue,用于调整权重，对同一个Queue全量消息的多个分流消费
            channel1.basicQos(12);
            channel2.basicQos(8);
            //自4.0+ 版本后无法再使用QueueingConsumer，而官方推荐使用DefaultConsumer
            final com.rabbitmq.client.Consumer consumer1 = new DefaultConsumer(channel1) {
                private int msg_count = 0;

                public int getMsg_count() {
                    return msg_count;
                }
                @Override
                public String toString() {
                    return "$classname{" +
                            "msg_count=" + msg_count +
                            '}';
                }
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
                        throws IOException {
                    msg_count++;//接收一条消息
                    try {
                        Thread.sleep(2000);
                        super.handleDelivery(consumerTag, envelope, properties, body);
                        String message = new String(body,"UTF-8");
                        //模拟消费端消费（处理）失败，没能进行消息确认的场景
                        if("Hello World!0123".equalsIgnoreCase(message)) {
                            throw new MessageFailException(Thread.currentThread().getName() + "->Message failed");
                        }
                        System.out.println(Thread.currentThread().getName() +"|" + queue + "->get msg:" + message);
                        //业务的最后一句手动确认，保证业务已成功执行，没有发生异常--一旦业务发生异常，这里执行不到，由异常处理或Broker端机制将消息死信，由专门消费端分析、处理
                        channel1.basicAck(envelope.getDeliveryTag(),false);//进行手动消息确认--没确认的消息驻留在内存，可以重复消费
                    } catch (MessageFailException e) {
                        System.out.println(e.getMessage());
                        //经实验：如果这里什么都不做，需要手动断开这个消费失败（即未能进行消息确认）的消费者连接，Broker才会判定消息需要重入队列，这样其他消费同一个Queue的消费端才能对未确认消息进行再度消费，否则未确认消息一直驻留Broker无法被消费
                        //如果这里主动reject/nack且设置最后一个参数requeue值为true,会通知Broker把未能消费成功（即未能进行消息确认）的消息重新放回队列供再次消费，但这种处理可能会造成异常消费的消息始终发生业务处理异常，循环放回队列进行不可能成功的处理，严重影响业务运营
                        //所以这里正确的处理办法应该是让消息死信（有几种情形，可以消费端reject/nack且requeue为false，可以设置消息过期时间，需要根据业务需求选择），建议在生产端（而不是消费端）declare队列时绑定死信Exchange，让Broker将死信消息放入专门的死信Exchange，进而路由到另外的队列，由专门的消费端线程统一处理
                        channel1.basicNack(envelope.getDeliveryTag(), false, false);//最后一个参数false:不重入队列，让消息成为死信，由专门消费端进行处理
                        //如果这里没能执行成功也不怕，未确认消息仍然存在，可事先设置足够长的过期时间，超时后自动死信，或在极端情况下断开该消费端，则其他消费端可再度消费该消息，进行相应处理
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        //放开一个latch
                        latch.countDown();
                    }
                }
            };
            consumerList.add(consumer1);
            final com.rabbitmq.client.Consumer consumer2 = new DefaultConsumer(channel2) {
                private int msg_count = 0;

                public int getMsg_count() {
                    return msg_count;
                }
                @Override
                public String toString() {
                    return "$classname{" +
                            "msg_count=" + msg_count +
                            '}';
                }
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
                        throws IOException {
                    msg_count++;//接收一条消息
                    try {
                        Thread.sleep(2000);
                        super.handleDelivery(consumerTag, envelope, properties, body);
                        String message = new String(body,"UTF-8");
                        //模拟消费端消费（处理）失败，没能进行消息确认的场景
                        if("Hello World!0123".equalsIgnoreCase(message)) {
                            throw new MessageFailException(Thread.currentThread().getName() + "->Message failed");
                        }
                        System.out.println(Thread.currentThread().getName() +"|" + queue + "->get msg:" + message);
                        //业务的最后一句手动确认，保证业务已成功执行，没有发生异常--一旦业务发生异常，这里执行不到，由异常处理或Broker端机制将消息死信，由专门消费端分析、处理
                        channel2.basicAck(envelope.getDeliveryTag(),false);//进行手动消息确认--没确认的消息驻留在内存，可以重复消费
                    } catch (MessageFailException e) {
                        System.out.println(e.getMessage());
                        //经实验：如果这里什么都不做，需要手动断开这个消费失败（即未能进行消息确认）的消费者连接，Broker才会判定消息需要重入队列，这样其他消费同一个Queue的消费端才能对未确认消息进行再度消费，否则未确认消息一直驻留Broker无法被消费
                        //如果这里主动reject/nack且设置最后一个参数requeue值为true,会通知Broker把未能消费成功（即未能进行消息确认）的消息重新放回队列供再次消费，但这种处理可能会造成异常消费的消息始终发生业务处理异常，循环放回队列进行不可能成功的处理，严重影响业务运营
                        //所以这里正确的处理办法应该是让消息死信（有几种情形，可以消费端reject/nack且requeue为false，可以设置消息过期时间，需要根据业务需求选择），建议在生产端（而不是消费端）declare队列时绑定死信Exchange，让Broker将死信消息放入专门的死信Exchange，进而路由到另外的队列，由专门的消费端线程统一处理
                        channel2.basicNack(envelope.getDeliveryTag(), false, false);//最后一个参数false:不重入队列，让消息成为死信，由专门消费端进行处理
                        //如果这里没能执行成功也不怕，未确认消息仍然存在，可事先设置足够长的过期时间，超时后自动死信，或在极端情况下断开该消费端，则其他消费端可再度消费该消息，进行相应处理
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        //放开一个latch
                        latch.countDown();
                    }
                }
            };
            consumerList.add(consumer2);
            new Thread(new Runnable() {//这个线程和执行Consumer回调的线程不是一个线程
                public void run() {
                    //监听队列，当b为true时，为自动提交（只要消息从队列中获取，无论消费者获取到消息后是否成功消息，都认为是消息已经成功消费），
                    // 当b为false时，为手动提交（消费者从队列中获取消息后，服务器会将该消息标记为不可用状态，等待消费者的反馈，
                    // 如果消费者一直没有反馈，那么该消息将一直处于不可用状态。
                    //如果选用自动确认,在消费者拿走消息执行过程中出现宕机时,消息可能就会丢失！！）
                    //使用channel.basicAck(envelope.getDeliveryTag(),false);进行消息确认
                    try {
                        //一个线程使用一个独占channel,一个独占consumer
                        channel1.basicConsume(queue,false,consumer1);//第二个参数false：设置为手动确认
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
            new Thread(new Runnable() {//这个线程和执行Consumer回调的线程不是一个线程
                public void run() {
                    //监听队列，当b为true时，为自动提交（只要消息从队列中获取，无论消费者获取到消息后是否成功消息，都认为是消息已经成功消费），
                    // 当b为false时，为手动提交（消费者从队列中获取消息后，服务器会将该消息标记为不可用状态，等待消费者的反馈，
                    // 如果消费者一直没有反馈，那么该消息将一直处于不可用状态。
                    //如果选用自动确认,在消费者拿走消息执行过程中出现宕机时,消息可能就会丢失！！）
                    //使用channel.basicAck(envelope.getDeliveryTag(),false);进行消息确认
                    try {
                        //一个线程使用一个独占channel,一个独占consumer
                        channel2.basicConsume(queue,false,consumer2);//第二个参数false：设置为手动确认
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("All messages consumed,latch released!");
        //接收完所有60条消息之后
        for (com.rabbitmq.client.Consumer consumer : consumerList) {
            System.out.println(consumer.toString());
        }
    }
}
