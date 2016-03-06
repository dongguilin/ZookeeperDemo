package com.guilin.zookeeper.demo;

import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by hadoop on 2016/3/6.
 * 使用TestingCluster模拟zk集群
 */
public class TestingClusterDemo {

    private static TestingCluster cluster = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        cluster = new TestingCluster(15);
        System.out.println("启动集群");
        cluster.start();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        System.out.println("关闭集群");
        cluster.stop();
    }

    @Test
    public void test1() throws IOException, InterruptedException {
        Thread.sleep(5000);

        TestingZooKeeperServer leader = null;
        for (TestingZooKeeperServer zs : cluster.getServers()) {
            System.out.print(zs.getInstanceSpec().getServerId() + "-");
            System.out.print(zs.getQuorumPeer().getServerState() + "-");
            System.out.println(zs.getInstanceSpec().getDataDirectory().getAbsolutePath());

            if (zs.getQuorumPeer().getServerState().equals("leading")) {
                leader = zs;
            }
        }

        System.out.println("杀死leader:" + leader.getInstanceSpec().getServerId());
        leader.kill();
        System.out.println("After leader kill:");

        for (TestingZooKeeperServer zs : cluster.getServers()) {
            System.out.print(zs.getInstanceSpec().getServerId() + "-");
            System.out.print(zs.getQuorumPeer().getServerState() + "-");
            System.out.println(zs.getInstanceSpec().getDataDirectory().getAbsolutePath());
        }

        Thread.sleep(5000);
        System.out.println();

        for (TestingZooKeeperServer zs : cluster.getServers()) {
            System.out.print(zs.getInstanceSpec().getServerId() + "-");
            System.out.print(zs.getQuorumPeer().getServerState() + "-");
            System.out.println(zs.getInstanceSpec().getDataDirectory().getAbsolutePath());
        }

    }

}
