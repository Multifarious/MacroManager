package com.fasterxml.slavedriver;

import com.twitter.common.zookeeper.ZooKeeperClient;

public abstract class SimpleListener {
    public void onJoin(ZooKeeperClient client) { }
    public void onLeave() { }
    public void shutdownWork(String workUnit) { }
}