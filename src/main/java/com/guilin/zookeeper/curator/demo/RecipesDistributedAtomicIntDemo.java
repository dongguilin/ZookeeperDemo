package com.guilin.zookeeper.curator.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hadoop on 2016/1/11.
 * 使用Curator实现分布式计数器
 */
public class RecipesDistributedAtomicIntDemo {

    private static String distatomicintPath = "/test/curator_recipes_distatomicint_path";

    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString("localhost:2181,localhost:2182,localhost:2183")
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    /**
     * DistributedAtomicInteger基本方法测试
     *
     * @throws Exception
     */
    @Test
    public void test1() throws Exception {
        client.start();

        //创建的节点distatomicintPath是永久节点
        DistributedAtomicInteger atomicInteger = new DistributedAtomicInteger(client, distatomicintPath, new RetryNTimes(3, 1000));

        //节点已存在时，调用initialize方法返回false
        System.out.println("initialize " + atomicInteger.initialize(10));

        System.out.println("+1 " + atomicInteger.increment().succeeded());
        System.out.println("+3 " + atomicInteger.add(3).succeeded());
        System.out.println("14+2 " + atomicInteger.compareAndSet(14, 16).succeeded());

        System.out.println("force set 5 ");
        atomicInteger.forceSet(5);
        System.out.println("try set 6 " + atomicInteger.trySet(6).succeeded());
        System.out.println("-1 " + atomicInteger.decrement().succeeded());
        System.out.println("-3 " + atomicInteger.subtract(3).succeeded());
        System.out.println("6-1-3=" + atomicInteger.get().postValue());

        client.close();
    }

    /**
     * DistributedAtomicInteger的构造方法和initialize都会创建永久节点
     *
     * @throws Exception
     */
    @Test
    public void test2() throws Exception {
        client.start();

        //先删除节点
        if (client.checkExists().forPath(distatomicintPath) != null) {
            client.delete().guaranteed().forPath(distatomicintPath);
        }
        //构造方法会创建节点
        DistributedAtomicInteger atomicInteger = new DistributedAtomicInteger(client, distatomicintPath, new RetryNTimes(3, 1000));
        System.out.println(atomicInteger.get().preValue() + "," + atomicInteger.get().postValue());//0,0
        AtomicValue<Integer> rc = atomicInteger.add(8);
        System.out.println(rc.succeeded() + " " + atomicInteger.get().preValue() + "," + atomicInteger.get().postValue());//true 8,8

        //删除节点
        client.delete().guaranteed().forPath(distatomicintPath);
        //initialize方法会创建节点
        System.out.println("initialize:" + atomicInteger.initialize(10));//initialize:true
        rc = atomicInteger.increment();
        System.out.println(rc.succeeded() + " " + atomicInteger.get().preValue() + "," + atomicInteger.get().postValue());//true 11,11

        client.close();
    }

    /**
     * 使用线程模拟分布式环境下increment
     *
     * @throws Exception
     */
    @Test
    public void test3() throws Exception {
        client.start();

        DistributedAtomicInteger atomicInteger = new DistributedAtomicInteger(client, distatomicintPath, new RetryNTimes(3, 1000));
        try {
            System.out.println("强制设置节点值为10");
            atomicInteger.forceSet(10);
        } catch (Exception e) {
            System.out.println("强制设置为10失败");
            e.printStackTrace();
        }

        final CountDownLatch lock = new CountDownLatch(20);

        ExecutorService pool = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 20; i++) {
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        DistributedAtomicInteger dai = new DistributedAtomicInteger(client, distatomicintPath, new RetryNTimes(3, 1000));
                        System.out.println(Thread.currentThread().getName() + " before:" + dai.get().preValue() + " after:" + dai.get().postValue());

                        //强制保证increment成功
                        boolean flag = false;
                        while (!flag) {
                            flag = dai.increment().succeeded();
                        }

                        Thread.sleep(new Random().nextInt(100));
                        System.out.println(Thread.currentThread().getName() + " before:" + dai.get().preValue() + " after:" + dai.get().postValue());
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.countDown();
                    }
                }
            });
        }
        lock.await();
        pool.shutdown();

        System.out.println(atomicInteger.get().postValue());
        client.close();
    }

}
