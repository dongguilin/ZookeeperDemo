package com.guilin.zookeeper.curator.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by hadoop on 2016/1/11.
 * 使用Curator的DistributedBarrier实现分布式Barrier，主动释放barrier模式
 */
public class RecipesDistributedBarrierDemo {

    private static String barrierPath = "/test/curator_recipes_barrier_path";

    private static DistributedBarrier barrier;

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 5; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    CuratorFramework client = CuratorFrameworkFactory.builder()
                            .connectString("localhost:2181")
                            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                            .build();
                    client.start();
                    barrier = new DistributedBarrier(client, barrierPath);
                    System.out.println(Thread.currentThread().getName() + " 号barrier设置");
                    try {
                        //设备barrier
                        barrier.setBarrier();
                        //等待barrier的释放
                        barrier.waitOnBarrier();
                        System.out.println("启动...");
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
        Thread.sleep(2000);
        //释放barrier，触发所有等待该barrier的线程
        barrier.removeBarrier();

    }

}
