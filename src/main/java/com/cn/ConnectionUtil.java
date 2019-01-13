package com.cn;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbit-learn
 * @description: 连接工厂类
 * @author: 535504
 * @create: 2018-04-26 15:10
 **/
public class ConnectionUtil {

    private static final String RABBIT_HOST = "localhost";

    private static final String RABBIT_USERNAME = "guest";

    private static final String RABBIT_PASSWORD = "guest";

    private static Connection connection = null;

    public static Connection getConnection() {
        if(connection == null) {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(RABBIT_HOST);
            connectionFactory.setUsername(RABBIT_USERNAME);
            connectionFactory.setPassword(RABBIT_PASSWORD);
            try {
                connection = connectionFactory.newConnection();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

}
