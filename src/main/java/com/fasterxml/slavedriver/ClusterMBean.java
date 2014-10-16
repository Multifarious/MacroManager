package com.fasterxml.slavedriver;

public interface ClusterMBean {
    public String join();
    public void shutdown();
    public void rebalance();
}
