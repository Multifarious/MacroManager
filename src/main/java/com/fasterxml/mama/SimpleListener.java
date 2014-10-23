package com.fasterxml.mama;

import com.fasterxml.mama.twitzk.ZooKeeperClient;

public abstract class SimpleListener {
    public void onJoin(ZooKeeperClient client) { }
    public void onLeave() { }
    public void shutdownWork(String workUnit) { }
}