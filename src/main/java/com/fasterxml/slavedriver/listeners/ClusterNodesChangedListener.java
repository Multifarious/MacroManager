package com.fasterxml.slavedriver.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.slavedriver.Cluster;
import com.fasterxml.slavedriver.NodeInfo;
import com.twitter.common.zookeeper.ZooKeeperMap;

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
            LOG.info("Nodes: %s".format(cluster.nodes.map(n => n._1).mkString(", ")));
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
