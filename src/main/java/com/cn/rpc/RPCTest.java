package com.cn.rpc;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @program: rabbitmq-learn
 * @description: RPC测试
 * @author: 535504
 * @create: 2018-04-27 10:11
 **/
public class RPCTest {

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        RPCClient rpcClient = new RPCClient();
        System.out.println(rpcClient.call("2"));
    }
}
