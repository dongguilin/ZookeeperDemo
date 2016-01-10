package com.guilin.zookeeper;

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
    private static String connectString = "aleiyeb:12181";
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
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, "init".getBytes());

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
        //创建一个节点，初始内容为空
        client.create().forPath(path);

        //创建一个节点，附带初始内容
        client.create().forPath(path, "init".getBytes());

        //创建一个临时节点，初始内容为空
        client.create().withMode(CreateMode.EPHEMERAL).forPath(path);

        //创建一个临时节点，并自动递归创建父节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
    }

    private void get() throws Exception {
        //读取一个节点的数据内容
        client.getData().forPath(path);

        //读取一个节点的数据内容，同时获取该节点的stat
        Stat stat = new Stat();
        client.getData().storingStatIn(stat).forPath(path);
    }

    private void update() throws Exception {
        //更新一个节点的数据内容
        Stat stat = client.setData().forPath(path);

        //更新一个节点的数据内容，强制指定版本进行更新
        client.setData().withVersion(stat.getVersion()).forPath(path);
    }

    private void delete() throws Exception {
        //删除一个节点，只能删除叶子节点
        client.delete().forPath(path);

        //删除一个节点，并递归删除其所有子节点
        client.delete().deletingChildrenIfNeeded().forPath(path);

        //删除一个节点，强制指定版本进行删除
        client.delete().withVersion(2).forPath(path);

        //删除一个节点，强制保证删除（只要客户端会话有效，那么Curator会在后台持续进行删除操作，直到节点删除成功）
        client.delete().guaranteed().forPath(path);
    }


}
