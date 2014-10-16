package com.fasterxml.slavedriver.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.slavedriver.Cluster;
import com.fasterxml.slavedriver.ClusterConfig;
import com.twitter.common.zookeeper.ZooKeeperMap;

public class VerifyIntegrityListener<T>
    implements ZooKeeperMap.Listener<T>
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final Cluster cluster;
    private final ClusterConfig clusterConfig;
    
    public VerifyIntegrityListener(Cluster c) {
        cluster = c;
        clusterConfig = c.getConfig();
    }        
    
    @Override
    public void nodeChanged(String nodeName, T data) {
        if (cluster.isInitialized()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(clusterConfig.workUnitName +
                        " IDs: "+cluster.allWorkUnits.keySet());
            }
            cluster.requestClaim();
            cluster.verifyIntegrity();
        }
    }

    @Override
    public void nodeRemoved(String nodeName) {
        if (cluster.isInitialized()) {
            cluster.requestClaim();
            cluster.verifyIntegrity();
        }
    }

}
