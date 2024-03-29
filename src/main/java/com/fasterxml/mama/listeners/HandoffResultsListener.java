package com.fasterxml.mama.listeners;

import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.mama.Cluster;
import com.fasterxml.mama.ClusterConfig;
import com.fasterxml.mama.NodeState;
import com.fasterxml.mama.util.ZKUtils;
import com.twitter.common.zookeeper.ZooKeeperMap;

public class HandoffResultsListener
    implements ZooKeeperMap.Listener<String>
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final Cluster cluster;
    private final ClusterConfig clusterConfig;
    
    public HandoffResultsListener(Cluster c) {
        cluster = c;
        clusterConfig = c.getConfig();
    }        

    @Override
    public void nodeChanged(String nodeName, String data) {
        apply(nodeName);
    }

    @Override
    public void nodeRemoved(String nodeName) {
        apply(nodeName);
    }

    /**
     * If I am the node which accepted this handoff, finish the job.
     * If I'm the node that requested to hand off this work unit to
     * another node, shut it down after <config> seconds.
     */
    private void apply(String workUnit) {
        if (!cluster.isInitialized()) {
            return;
        }
        if (iRequestedHandoff(workUnit)) {
            String str = cluster.getHandoffResult(workUnit);
            LOG.info("Handoff of {} to {} completed. Shutting down {} in {} seconds.", workUnit,
                    (str == null) ? "(None)" : str,
                    workUnit, clusterConfig.handoffShutdownDelay);
            ZKUtils.delete(cluster.zk, String.format("/%s/handoff-requests/%s", cluster.name, workUnit));
            cluster.schedule(shutdownAfterHandoff(workUnit), clusterConfig.handoffShutdownDelay, TimeUnit.SECONDS);
        }
    }
    
    /**
     * Determines if this node requested handoff of a work unit to someone else.
     * I have requested handoff of a work unit if it's currently a member of my active set
     * and its destination node is another node in the cluster.
     */
    private boolean iRequestedHandoff(String workUnit)
    {
        String destinationNode = cluster.getHandoffResult(workUnit);
        return (destinationNode != null)
                && cluster.myWorkUnits.contains(workUnit)
                && !destinationNode.equals("")
                && !cluster.isMe(destinationNode);
    }

    /**
     * Builds a runnable to shut down a work unit after a configurable delay once handoff
     * has completed. If the cluster has been instructed to shut down and the last work unit
     * has been handed off, this task also directs this Ordasity instance to shut down.
     */
    private Runnable shutdownAfterHandoff(final String workUnit)
    {
        final Cluster cluster = this.cluster;
        final Logger log = LOG;
        return new Runnable() {
            @Override
            public void run() {
                String str = cluster.getHandoffResult(workUnit);
                log.info("Shutting down {} following handoff to {}.",
                        workUnit, (str == null) ? "(None)" : str);
                cluster.shutdownWork(workUnit, /*doLog =*/ false);

                if (cluster.hasState(NodeState.Draining) && cluster.myWorkUnits.isEmpty()) {
                    cluster.shutdown();
                }
            }
        };
    }

    /**
     * Completes the process of handing off a work unit from one node to the current one.
     * Attempts to establish a final claim to the node handed off to me in ZooKeeper, and
     * repeats execution of the task every two seconds until it is complete.
     */
    public void finishHandoff(final String workUnit) throws InterruptedException
    {
        String unitId = cluster.workUnitMap.get(workUnit);
        LOG.info("Handoff of {} to me acknowledged. Deleting claim ZNode for {} and waiting for {} to shutdown work.",
                workUnit, workUnit,
                ((unitId == null) ? "(None)" : unitId));

        final String path = cluster.workUnitClaimPath(workUnit);

        Stat stat = ZKUtils.exists(cluster.zk, path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // Don't really care about the type of event here - call unconditionally to clean up state
                completeHandoff(workUnit, path);
            }
        });
        // Unlikely that peer will have already deleted znode, but handle it regardless
        if (stat == null) {
            LOG.warn("Peer already deleted znode of {}", workUnit);
            completeHandoff(workUnit, path);
        }
    }

    protected void completeHandoff(String workUnit, String path)
    {
        try {
            LOG.info("Completing handoff of {}", workUnit);
            if (ZKUtils.createEphemeral(cluster.zk, path, cluster.myNodeID) || cluster.znodeIsMe(path)) {
                LOG.info("Handoff of {} to me complete. Peer has shut down work.", workUnit);
            } else {
                LOG.warn("Failed to completed handoff of {} - couldn't create ephemeral node", workUnit);
            }
        } catch (Exception e) {
            LOG.error("Error completing handoff of "+workUnit+" to me.", e);
        } finally {
            ZKUtils.delete(cluster.zk, "/" + cluster.name + "/handoff-result/" + workUnit);
            cluster.claimedForHandoff.remove(workUnit);
        }
    }
}
