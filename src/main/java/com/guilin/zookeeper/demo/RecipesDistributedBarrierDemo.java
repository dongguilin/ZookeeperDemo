package com.guilin.zookeeper.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Created by hadoop on 2016/1/11.
 * 使用Curator实现分布式Barrier
 */
public class RecipesDistributedBarrierDemo {

    private static String barrierPath = "/curator_recipes_barrier_path";

    private static DistributedBarrier barrier;

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 5; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    CuratorFramework client = CuratorFrameworkFactory.builder()
                            .connectString("aleiyeb:12181")
                            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                            .build();
                    client.start();
                    barrier = new DistributedBarrier(client, barrierPath);
                    System.out.println(Thread.currentThread().getName() + " 号barrier设置");
                    try {
                        barrier.setBarrier();
                        barrier.waitOnBarrier();
                        System.out.println("启动...");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }).start();
        }
        Thread.sleep(2000);
        barrier.removeBarrier();

    }

}
