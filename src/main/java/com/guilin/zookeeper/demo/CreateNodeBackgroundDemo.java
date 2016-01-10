package com.guilin.zookeeper.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hadoop on 2016/1/10.
 * 使用异步方式创建节点
 */
public class CreateNodeBackgroundDemo {

    private static String path = "/zk-book";

    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString("aleiyeb:12181")
            .sessionTimeoutMs(5000)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    private static CountDownLatch semaphore = new CountDownLatch(2);

    private static ExecutorService pool = Executors.newFixedThreadPool(2);

    @Test
    public void test1() throws Exception {
        client.start();

        System.out.println("Main thread:" + Thread.currentThread().getName());

        //传入自定义的Executor
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        System.out.println("event[code:" + curatorEvent.getResultCode() + ", type:" + curatorEvent.getType() + "]");
                        System.out.println("Thread of processResult:" + Thread.currentThread().getName());
                        semaphore.countDown();
                    }
                }, pool).forPath(path, "init".getBytes());


        //没有传入自定义的Executor，异步通知事件处理由EventThread这个线程来处理
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                        System.out.println("event[code:" + curatorEvent.getResultCode() + ", type:" + curatorEvent.getType() + "]");
                        System.out.println("Thread of processResult:" + Thread.currentThread().getName());
                        semaphore.countDown();
                    }
                }).forPath(path, "init".getBytes());

        semaphore.await();
        pool.shutdown();

    }

}
