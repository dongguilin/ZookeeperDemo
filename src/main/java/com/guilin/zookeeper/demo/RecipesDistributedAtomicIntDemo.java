package com.guilin.zookeeper.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;

/**
 * Created by hadoop on 2016/1/11.
 * 使用Curator实现分布式计数器
 */
public class RecipesDistributedAtomicIntDemo {

    private static String distatomicintPath = "curator_recipes_distatomicint_path";

    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString("aleiyeb:12181")
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    public static void main(String[] args) throws Exception {
        client.start();

        DistributedAtomicInteger atomicInteger = new DistributedAtomicInteger(client, distatomicintPath, new RetryNTimes(3, 1000));
        AtomicValue<Integer> rc = atomicInteger.add(8);
        System.out.println("Result:" + rc.succeeded());
    }
}
