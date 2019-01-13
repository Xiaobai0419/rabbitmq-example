package com.cn.simple;

import com.cn.ConnectionUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbit-learn
 * @description: 生产者
 * @author: 535504
 * @create: 2018-04-26 15:02
 **/
public class Producer {

    private static final String QUEUE_NAME="test_queue";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        //获取连接
        Connection connection = ConnectionUtil.getConnection();
        System.out.println(connection);
        //创建通道
        Channel channel = connection.createChannel(1);
        /*
         * 声明（创建）队列
         * 参数1：队列名称
         * 参数2：为true时server重启队列不会消失
         * 参数3：队列是否是独占的，如果为true只能被一个connection使用，其他连接建立时会抛出异常
         * 参数4：队列不再使用时是否自动删除（没有连接，并且没有未处理的消息)
         * 参数5：建立队列时的其他参数
         */
        channel.queueDeclare(QUEUE_NAME,true,false,false,null);//第二个参数true:持久化队列，防止消息丢失，同时发送的消息也应设置持久化，但影响性能，靠多队列和集群扩展性能
        String message = "Hello World!";
        for (int i = 0; i < 20; i++) {
            message = message + i;
            //第一个参数：Exchange,建议为了保证消息不丢失，也设置为持久化，这里使用默认Exchange,是一个direct类型的
            channel.basicPublish("",QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes());//第三个参数：消息持久化，和队列持久化一起保证消息不丢失，同时结合broker端镜像队列设置（副本机制）、集群多节点热备
            System.out.println("生产者 send ："+message);//即使消息是持久化的，如果被成功消费，也会在Broker中永久删除，只有消费失败的持久化消息才会被永久保留，直到消费成功
            Thread.sleep(1000);
        }
        //channel.close();
        //connection.close();
    }

}
