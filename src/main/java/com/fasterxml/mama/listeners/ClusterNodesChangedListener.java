package com.fasterxml.mama.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.mama.Cluster;
import com.fasterxml.mama.NodeInfo;
import com.fasterxml.mama.twitzk.ZooKeeperMap;
import com.fasterxml.mama.util.Strings;

public class ClusterNodesChangedListener
    implements ZooKeeperMap.Listener<NodeInfo>
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final Cluster cluster;
    
    public ClusterNodesChangedListener(Cluster c) {
        cluster = c;
    }        

    @Override
    public void nodeChanged(String nodeName, NodeInfo data) {
        if (cluster.isInitialized()) {
            LOG.info("Nodes: {}", Strings.mkstringForKeys(cluster.nodes, ", "));
            cluster.requestClaim();
            cluster.verifyIntegrity();
        }
    }

    @Override
    public void nodeRemoved(String nodeName) {
        if (cluster.isInitialized()) {
            LOG.info("{} has left the cluster.", nodeName);
            cluster.requestClaim();
            cluster.verifyIntegrity();
        }
    }     
}
