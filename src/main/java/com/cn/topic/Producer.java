package com.cn.topic;/**
 * @Description: Created by xpl on 2018-04-26 21:39.
 */

import com.cn.ConnectionUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by xpl on 2018-04-26 21:39
 **/

public class Producer {

    private static final String EXCHANGE_NAME = "test_exchange_topic";

    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = ConnectionUtil.getConnection();
        Channel channel = connection.createChannel();
        //声明交换机
        channel.exchangeDeclare(EXCHANGE_NAME,"topic");
        String message = "匹配insert";
        channel.basicPublish(EXCHANGE_NAME,"order.update",false,false,null,message.getBytes());

        channel.close();
        connection.close();
    }
}
