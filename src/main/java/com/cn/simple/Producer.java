package com.cn.simple;

import com.cn.ConnectionUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbit-learn
 * @description: 生产者
 * @author: 535504
 * @create: 2018-04-26 15:02
 **/
public class Producer {//测试新一批生产前，先关闭所有消费端，避免即时消费，控制台看不到所有消息的堆积--这里模拟的是由生产者先连接，创建Exchange,Queue,绑定，并发送消息的场景，实际应用中注意消费者消费能力与生产者的对等，不要大量堆积消息，这不是RabbitMQ的优势！！

    private static final String DLX="dlx_exchange";//死信Exchange
    private static final String DLX_QUEUE="dlx_queue";//死信队列
    private static final String ROUTING_KEY="test_queue";
    //模拟从数据库或缓存中查询的需要创建的Exchange,Queue，管理者实时根据吞吐量需要向数据库或缓存添加新的Exchange,Queue名称
    private static String[] new_exchanges = {"test_exchange1","test_exchange2","test_exchange3"};
    private static String[] new_queues = {"test_queue1","test_queue2","test_queue3"};

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        //获取连接
        Connection connection = ConnectionUtil.getConnection();
        System.out.println(connection);
        //创建通道
        Channel channel = connection.createChannel(1);
        //创建死信Exchange,并绑定一个队列作为死信队列--用于失败保单的专门处理
        channel.exchangeDeclare(DLX, "direct", true);
        channel.queueDeclare(DLX_QUEUE,true,false,false,null);
        channel.queueBind(DLX_QUEUE, DLX, ROUTING_KEY);//从DLX订阅死信（使用ROUTING_KEY绑定DLX）
        //普通队列的死信参数--失败（因消费端保单处理异常而未确认的）消息应进入的死信Exchange
        Map<String, Object> queue_args = new HashMap<String, Object>();
        queue_args.put("x-dead-letter-exchange", DLX);//你也可以为这个DLX指定routing key，如果没有特殊指定，则使用原队列的routing key
        //生产者每次发消息，都动态地从数据库动态查询所有，包括新加的Exchange,Queue，进行批量创建、绑定、发送,已存在的则继续使用--暂定，具体谁创建看业务需要，甚至可事先或随时通过控制台创建好Exchange,Queue和routingKey绑定
        for(String exchange : new_exchanges) {
            //创建了一个durable, non-autodelete并且绑定类型为direct的exchange
            channel.exchangeDeclare(exchange, "direct", true);//第三个参数true:持久化Exchange,为了保证消息重启不丢，Exchange建议也持久化
        }
        for(String queue : new_queues) {
            /*
             * 声明（创建）队列
             * 参数1：队列名称
             * 参数2：为true时server重启队列不会消失
             * 参数3：队列是否是独占的，如果为true只能被一个connection使用，其他连接建立时会抛出异常
             * 参数4：队列不再使用时是否自动删除（没有连接，并且没有未处理的消息)
             * 参数5：建立队列时的其他参数--这里加入了死信Exchange
             */
            channel.queueDeclare(queue,true,false,false,queue_args);//第二个参数true:持久化队列，防止消息丢失，同时发送的消息也应设置持久化，但影响性能，靠多队列和集群扩展性能
        }
        for(int i = 0;i < new_exchanges.length;i++) {
            //使用routing key将queue和exchange绑定起来--Exchange将匹配routingKey的消息路由到这个queue
            //注意：如果这里使用同一个ROUTING_KEY同时将两个Queue绑定到一个Exchange,即使Exchange是direct类型，两个Queue接收的都是Exchange里面的全量消息（比如发往Exchange20条，两个Queue都是这20条），如果需要分流全量消息，需要使用不同的消费端消费同一个Queue
            channel.queueBind(new_queues[i], new_exchanges[i], ROUTING_KEY);//不同的Exchange,Queue组也可使用不同的ROUTING_KEY进行绑定，只要消息分流发送的ROUTING_KEY设定与不同组绑定ROUTING_KEY匹配即可
            System.out.println("Bind:" + new_exchanges[i] + "->" + new_queues[i] + "|routing:" + ROUTING_KEY);
        }
        for(String exchange : new_exchanges) {
            String message = "Hello World!";
            for (int i = 0; i < 20; i++) {
                message = message + i;
                //第一个参数：Exchange,建议为了保证消息不丢失，也设置为持久化，这里使用默认Exchange,是一个direct类型的
//            channel.basicPublish("",QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes());
                //第二个参数routingKey:设置消息的路由key,Exchange将据此将消息路由到匹配消息routingKey的绑定queue（bindingKey）
                channel.basicPublish(exchange,ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes());//第三个参数：消息持久化，和队列、Exchange持久化一起保证消息不丢失，同时结合broker端镜像队列设置（副本机制）、集群多节点热备
                System.out.println("To:" + exchange + ",生产者 send ："+message);//即使消息是持久化的，如果被成功消费，也会在Broker中永久删除，只有消费失败的持久化消息才会被永久保留，直到消费成功
                Thread.sleep(1000);
            }
        }
        //channel.close();
        //connection.close();
    }

}
