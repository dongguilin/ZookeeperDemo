package com.guilin.zookeeper.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

/**
 * Created by hadoop on 2016/1/10.
 * Master选举
 */
public class RecipesMasterSelectDemo {

    private static String masterPath = "/curator_recipes_master_path";

    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString("localhost:2181")
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    @Test
    public void test1() throws InterruptedException {
        client.start();

        //一旦执行完takeLeadership方法，Curator就会立即释放Master权利，然后重新开始新一轮的Master选举
        //当一个应用程序完成Master逻辑后，另一个应用程序的takeLeadership方法才会被调用
        //即当一个应用实例成为Master后，其他应用实例会进入等待，直到当前Master挂了或退出后才会开始选举新的Master
        LeaderSelector selector = new LeaderSelector(client, masterPath, new LeaderSelectorListenerAdapter() {
            @Override
            public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                System.out.println("成为Master角色");
                Thread.sleep(3000);
                System.out.println("完成Master操作，释放Master权利");
            }
        });
        selector.autoRequeue();
        selector.start();
        Thread.sleep(Integer.MAX_VALUE);

    }


}
