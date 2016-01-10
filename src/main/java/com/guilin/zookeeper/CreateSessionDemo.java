package com.guilin.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by hadoop on 2016/1/6.
 */
public class CreateSessionDemo {

    private String connectString = "localhost:2181";

    @Test
    public void test1(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString,5000,3000,retryPolicy);
        client.start();
        Assert.assertEquals(CuratorFrameworkState.STARTED, client.getState());
        client.close();
        Assert.assertEquals(CuratorFrameworkState.STOPPED,client.getState());
    }

    //Fluent风格
    @Test
    public void test2(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(connectString)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .retryPolicy(retryPolicy).build();
        client.start();
        Assert.assertEquals(CuratorFrameworkState.STARTED, client.getState());
        client.close();
        Assert.assertEquals(CuratorFrameworkState.STOPPED,client.getState());
    }

}
