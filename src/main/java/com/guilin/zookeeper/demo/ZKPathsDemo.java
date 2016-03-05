package com.guilin.zookeeper.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.ZooKeeper;

/**
 * Created by hadoop on 2016/1/11.
 * ZKPaths提供了一些简单的API来构建ZNode路径、递归创建和删除节点等
 */
public class ZKPathsDemo {

    private static String path = "/curator_zkpath_sample";

    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString("aleiyeb:12181")
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    public static void main(String[] args) throws Exception {
        client.start();
        ZooKeeper zooKeeper = client.getZookeeperClient().getZooKeeper();
        System.out.println(ZKPaths.fixForNamespace(path, "sub"));
        System.out.println(ZKPaths.makePath(path, "sub"));
        System.out.println(ZKPaths.getNodeFromPath(path + "/sub1"));

        ZKPaths.PathAndNode pn = ZKPaths.getPathAndNode(path + "/sub1");
        System.out.println(pn.getPath());
        System.out.println(pn.getNode());

        String dir1 = path + "/child1";
        String dir2 = path + "/child2";
        ZKPaths.mkdirs(zooKeeper, dir1);
        ZKPaths.mkdirs(zooKeeper, dir2);
        System.out.println(ZKPaths.getSortedChildren(zooKeeper, path));
        ZKPaths.deleteChildren(zooKeeper, path, true);
    }
}
