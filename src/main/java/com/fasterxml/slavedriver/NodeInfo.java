package com.fasterxml.slavedriver;

public class NodeInfo {
    public final String state;
    public final long connectionId;

    public NodeInfo(String state, long connectionId) {
        this.state = state;
        this.connectionId = connectionId;
    }
}
