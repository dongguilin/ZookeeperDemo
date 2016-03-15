package com.guilin.zookeeper.demo.leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by guilin1 on 16/3/8.
 * Master选举
 */
public class LeaderLatchDemo {

    private static TestingCluster cluster = null;

    private static final String LEADER_PATH = "/test/leader";

    private static LeaderLatch leaderLatch;

    @BeforeClass
    public static void beforeClass() throws Exception {
        cluster = new TestingCluster(15);
        System.out.println("启动集群");
        cluster.start();
    }

    @AfterClass
    public static void afterClass() throws IOException, InterruptedException {
        System.out.println("关闭集群");
        cluster.stop();
    }

    @Test
    public void test1() throws Exception {

        new LeaderLatchDemo().initialized(cluster.getConnectString());

        //输出集群中各节点详情
        TestingZooKeeperServer leader = null;
        for (TestingZooKeeperServer zs : cluster.getServers()) {
            System.out.print(zs.getInstanceSpec().getServerId() + "-");
            System.out.print(zs.getQuorumPeer().getServerState() + "-");
            System.out.println(zs.getInstanceSpec().getDataDirectory().getAbsolutePath());

            //获取leader
            if (zs.getQuorumPeer().getServerState().equals("leading")) {
                leader = zs;
            }
        }

        //杀死leader
        System.out.println("杀死leader:" + leader.getInstanceSpec().getServerId());
        System.out.println(leaderLatch.getLeader().isLeader());
        leader.kill();


        System.out.println("After leader kill:");

        //同步等待leader选举成功
        leaderLatch.await(10, TimeUnit.SECONDS);

        //输出集群中各节点详情
        for (TestingZooKeeperServer zs : cluster.getServers()) {
            System.out.print(zs.getInstanceSpec().getServerId() + "-");
            System.out.print(zs.getQuorumPeer().getServerState() + "-");
            System.out.println(zs.getInstanceSpec().getDataDirectory().getAbsolutePath());
        }

        //等待一段时间后结束
        Thread.currentThread().join(1000 * 3);

    }


    public void initialized(String connecString) throws Exception {
        try {
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .connectString(connecString)
                    .connectionTimeoutMs(1000 * 60)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .build();
            leaderLatch = new LeaderLatch(client, LEADER_PATH, "guilin");

            leaderLatch.addListener(new SayHello(leaderLatch));
            leaderLatch.addListener(new SayWorld(leaderLatch));

            client.start();
            leaderLatch.start();

            //同步等待leader选举成功
            leaderLatch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.out.println("中断线程");
            destroyed();
            throw new Exception("创建ZK选举对象失败，请检查ZK是否连接正常", e);
        }

    }

    public void destroyed() throws Exception {
        try {
            leaderLatch.close();
        } catch (IOException e) {
            throw new Exception("关闭ZK选举对象失败", e);
        }
    }

}
