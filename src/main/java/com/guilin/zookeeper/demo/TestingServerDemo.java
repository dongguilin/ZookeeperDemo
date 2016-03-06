package com.guilin.zookeeper.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by hadoop on 2016/3/6.
 * 使用TestingServer模拟zookeeper服务
 */
public class TestingServerDemo {

    private static String path = "/test/testingserver";

    private static TestingServer server = null;
    private static CuratorFramework client = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        server = new TestingServer(12181, new File("E:/Test/zookeeper/data"));
        client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .sessionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();

        System.out.println("打开连接");
        client.start();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        System.out.println("关闭连接");
        client.close();
        System.out.println("关闭服务");
        server.close();
    }

    @Test
    public void test1() throws Exception {
        client.create().creatingParentsIfNeeded().forPath(path);
        client.setData().forPath(path);
        System.out.println(new String(client.getData().forPath(path)));
    }


}
