package com.fasterxml.slavedriver;

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
}
