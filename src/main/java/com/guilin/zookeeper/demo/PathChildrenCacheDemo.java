package com.guilin.zookeeper.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

/**
 * Created by hadoop on 2016/1/10.
 * 事件监听
 * PathChildrenCache用于监听指定Zookeeper数据节点的子节点变化情况，包括新增子节点、子节点数据变更、子节点删除
 * Curator无法对二级节点进行事件监听，也就是说，如果使用PathChildrenCache对/zk-book进行监听，
 * 那么当/zk-book/c1/c2节点被创建或删除的时候，是无法触发子节点变更事件的
 */
public class PathChildrenCacheDemo {

    private static String path = "/zk-book";

    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString("localhost:2181")
            .sessionTimeoutMs(5000)
            .connectionTimeoutMs(3000)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    @Test
    public void test1() throws Exception {
        client.start();

        PathChildrenCache cache = new PathChildrenCache(client, path, true);
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED:
                        System.out.println("child added," + event.getData().getPath());
                        break;
                    case CHILD_UPDATED:
                        System.out.println("child updated," + event.getData().getPath());
                        break;
                    case CHILD_REMOVED:
                        System.out.println("child removed," + event.getData().getPath());
                        break;
                    default:
                        break;
                }
            }
        });

        if (client.checkExists().forPath(path) == null) {
            client.create().withMode(CreateMode.PERSISTENT).forPath(path);
            Thread.sleep(1000);
        }
        if (client.checkExists().forPath(path + "/c1") == null) {
            client.create().withMode(CreateMode.PERSISTENT).forPath(path + "/c1");
            Thread.sleep(1000);
        }
        client.setData().forPath(path + "/c1", "test".getBytes());
        client.delete().forPath(path + "/c1");
        Thread.sleep(1000);
        client.delete().forPath(path);
        Thread.sleep(Integer.MAX_VALUE);
    }

}
