package com.fasterxml.mama;

import java.net.InetAddress;

public class ClusterConfig
{
    private final static String LOCALHOST;
    static {
        String str = "N/A";
        try {
            str = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) { // not clean but...
            System.err.println("ERROR: failed to get localhost name: "+e);
        }
        LOCALHOST = str;
    }
    
    public String hosts = "";
    public boolean enableAutoRebalance = true;
    public int autoRebalanceInterval = 60;
    public int drainTime = 60;
    public boolean useSmartBalancing = false;
    public long zkTimeout = 3000;
    public String workUnitName = "work-units";
    public String workUnitShortName = "work";
    public String nodeId = LOCALHOST;
    public boolean useSoftHandoff = false;
    public long handoffShutdownDelay = 10L;

    public ClusterConfig() { }

    public ClusterConfig hosts(String v) { hosts = v; return this; }
    public ClusterConfig enableAutoRebalance(boolean v) { enableAutoRebalance = v; return this; }
    public ClusterConfig autoRebalanceInterval(int v) { autoRebalanceInterval = v; return this; }
    public ClusterConfig drainTime(int v) { drainTime = v; return this; }
    public ClusterConfig useSmartBalancing(boolean v) { useSmartBalancing = v; return this; }
    public ClusterConfig zkTimeout(long v) { zkTimeout = v; return this; }
    public ClusterConfig workUnitName(String v) { workUnitName = v; return this; }
    public ClusterConfig workUnitShortName(String v) { workUnitShortName = v; return this; }
    public ClusterConfig nodeId(String v) { nodeId = v; return this; }
    public ClusterConfig useSoftHandoff(boolean v) { useSoftHandoff = v; return this; }
    public ClusterConfig handoffShutdownDelay(long v) { handoffShutdownDelay = v; return this; }
}
