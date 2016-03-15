package com.guilin.zookeeper.demo.leader;

import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

/**
 * Created by guilin1 on 16/3/8.
 */
public class SayHello implements LeaderLatchListener {

    private LeaderLatch leaderLatch;

    public SayHello(LeaderLatch leaderLatch) {
        this.leaderLatch = leaderLatch;
    }

    @Override
    public void isLeader() {
        try {
            System.out.println(leaderLatch.getLeader().isLeader() + " " + leaderLatch.getLeader().toString() + " " +
                    Thread.currentThread().getName() + " say hello!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void notLeader() {
        try {
            System.out.println(leaderLatch.getLeader().isLeader() + " " + leaderLatch.getLeader().toString() + " " +
                    Thread.currentThread().getName() + " SayHello goodbye!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
