package com.fasterxml.slavedriver;

public interface ClusterMBean {
    public String join() throws InterruptedException;
    public void shutdown();
    public void rebalance() throws InterruptedException;
}
