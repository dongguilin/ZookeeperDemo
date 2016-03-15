package com.guilin.zookeeper.curator.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

/**
 * Created by hadoop on 2016/1/10.
 * 事件监听
 * NodeCache用于监听指定Zookeeper数据节点本身的变化（数据节点的create、setData操作）
 */
public class NodeCacheDemo {

    private static String path = "/zk-book/nodecache";

    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString("localhost:2181")
            .sessionTimeoutMs(5000)
            .connectionTimeoutMs(3000)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    @Test
    public void test1() throws Exception {
        client.start();

//        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
//                .forPath(path, "init".getBytes());

        //NodeCache不仅可以用于监听数据节点的内容的变更，也能监听指定节点是否存在。
        //如果原本节点不存在，那么Cache就会在节点被创建后触发NodeCacheListener，但是，如果该数据节点被删除，那么Curator就无法触发NodeCacheListener
        final NodeCache cache = new NodeCache(client, path, false);
        //如果设置为true，那么NodeCache在第一次启动的时候就会立刻从Zookeeper上读取对应节点的数据内容，并保存在Cache中
        //如果设置为false，启动时，节点有数据就会触发该监听
        cache.start(false);
        cache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("Node data update, new data:" + new String(cache.getCurrentData().getData()));
            }
        });

//        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
//                .forPath(path, "init".getBytes());

//        Thread.sleep(5000);
//        client.setData().forPath(path, "heihei".getBytes());
//        client.setData().forPath(path, "heihei".getBytes());
//        client.setData().forPath(path, "呵呵".getBytes());
        Thread.sleep(1000);

//        client.create().forPath(path + "/1");//如果父节点是临时的话，就不能构建其子节点

//        client.delete().deletingChildrenIfNeeded().forPath(path);
        Thread.sleep(Integer.MAX_VALUE);
    }


}
