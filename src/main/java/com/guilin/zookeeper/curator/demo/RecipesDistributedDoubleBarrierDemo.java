package com.guilin.zookeeper.curator.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by hadoop on 2016/1/11.
 * 使用Curator的DistributedDoubleBarrier实现一个分布式Barrier，并控制其同时进入和退出
 * 线程自发触发Barrier释放的模式
 */
public class RecipesDistributedDoubleBarrierDemo {

    private static String barrierPath = "/test/curator_recipes_barrier_path";

    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    CuratorFramework client = CuratorFrameworkFactory.builder()
                            .connectString("localhost:2181")
                            .connectionTimeoutMs(1000 * 30)
                            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                            .build();
                    client.start();
                    DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, barrierPath, 5);
                    try {
                        Thread.sleep(Math.round(Math.random() * 3000));
                        System.out.println(Thread.currentThread().getName() + " 号进入barrier");
                        //每个Barrier的参与者都会在调用enter方法之后进行等待，一旦进入Barrier的成员数达到5个后，所有的成员会被同时触发进入
                        barrier.enter();
                        System.out.println("启动...");
                        Thread.sleep(Math.round(Math.random() * 3000));
                        //等待，一旦准备退出的成员数达到5个后，所有的成员同样会被同时触发退出
                        barrier.leave();
                        System.out.println("退出...");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (client != null) {
                            client.close();
                        }
                    }
                }
            }).start();
        }
    }

}
