package com.guilin.zookeeper.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by hadoop on 2016/1/9.
 */
public class CrudNodeDemo {

    private static CuratorFramework client;
    private static String connectString = "localhost:2181,localhost:2182,localhost:2183";
    private String path = "/zk-book/c1";

    @BeforeClass
    public static void beforeClass() {
        client = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        client.start();
        System.out.println(client.getState());
    }

    @AfterClass
    public static void afterClass() {
        client.close();
        System.out.println(client.getState());
    }

    @Test
    public void test1() throws Exception {
        //默认创建的是PERSISTENT节点
//        if (client.checkExists().forPath(path) == null) {
//            client.create().creatingParentsIfNeeded().forPath(path, "init".getBytes());
//        }

//        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, "init".getBytes());
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, "init".getBytes());
//        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, "init".getBytes());

//        Thread.sleep(60*1000);

        //获取数据
        Stat stat = new Stat();
        byte[] buff = client.getData().storingStatIn(stat).forPath(path);
        System.out.println(new String(buff));

        //更新数据
        stat = client.setData().withVersion(stat.getVersion()).forPath(path, "hello".getBytes());

        client.setData().withVersion(stat.getVersion()).forPath(path, "world".getBytes());


//        client.delete().deletingChildrenIfNeeded().withVersion(stat.getVersion()).forPath(path);
    }

    private void create() throws Exception {
        //创建一个节点，初始内容为客户端IP地址
        client.create().forPath(path);

        //创建一个节点，附带初始内容
        client.create().forPath(path, "init".getBytes());

        //创建一个临时节点，初始内容为空
        client.create().withMode(CreateMode.EPHEMERAL).forPath(path);

        //创建一个临时节点，并自动递归创建父节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
    }

    @Test
    public void createNode() throws Exception {
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/hello/a1/a2", "".getBytes());
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/hello/a1/a2", "".getBytes());

        client.create().withMode(CreateMode.EPHEMERAL).forPath("/e", "".getBytes());

        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/p/a", "".getBytes());
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/p/b", "".getBytes());
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/p/b", "".getBytes());
        Thread.currentThread().join();
    }

    @Test
    public void get() throws Exception {
        //读取一个节点的数据内容
        String data = new String(client.getData().forPath(path));
        System.out.println(data);

        //读取一个节点的数据内容，同时获取该节点的stat
        Stat stat = new Stat();
        data = new String(client.getData().storingStatIn(stat).forPath(path));
        System.out.println(data);
        System.out.println(stat);

        data = new String(client.getData().forPath("/p/a"));
        System.out.println(null == data);
    }

    @Test
    public void update() throws Exception {
        //更新一个节点的数据内容
        Stat stat = client.setData().forPath(path);

        //更新一个节点的数据内容，强制指定版本进行更新
        client.setData().withVersion(stat.getVersion()).forPath(path, "hello,world".getBytes());
    }

    @Test
    public void delete() throws Exception {
        //删除一个节点，只能删除叶子节点
//        client.delete().forPath(path);

        //删除一个节点，并递归删除其所有子节点
//        client.delete().deletingChildrenIfNeeded().forPath(path);

        //删除一个节点，强制指定版本进行删除
//        client.delete().withVersion(6).forPath(path);

        //删除一个节点，强制保证删除（只要客户端会话有效，那么Curator会在后台持续进行删除操作，直到节点删除成功）
        client.delete().guaranteed().forPath(path);
    }


}
