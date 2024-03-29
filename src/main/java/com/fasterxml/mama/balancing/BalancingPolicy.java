package com.fasterxml.mama.balancing;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.mama.Cluster;
import com.fasterxml.mama.ClusterConfig;
import com.fasterxml.mama.NodeInfo;
import com.fasterxml.mama.NodeState;
import com.fasterxml.mama.util.Strings;
import com.fasterxml.mama.util.ZKUtils;
import com.twitter.common.zookeeper.ZooKeeperClient.ZooKeeperConnectionException;

public abstract class BalancingPolicy
{
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected final Cluster cluster;
    protected final ClusterConfig config;
    
    public BalancingPolicy(Cluster c) {
        cluster = c;
        config = c.getConfig();
    }        

    public abstract void claimWork() throws InterruptedException;
    public abstract void rebalance() throws InterruptedException;

    public void shutdown() { }
    public void onConnect() { }
    public void onShutdownWork(String workUnit) { }

    public int activeNodeSize()
    {
        int count = 0;
        final String STARTED = NodeState.Started.toString();
        
        for (NodeInfo n : cluster.nodes.values()) {
            if (n != null && STARTED.equals(n.state)) {
                ++count;
            }
        }
        return count;
    }

    /**
     * Returns a set of work units which are unclaimed throughout the cluster.
     */
    public Set<String> getUnclaimed() {
        synchronized (cluster.allWorkUnits) {
            LinkedHashSet<String> result = new LinkedHashSet<String>(cluster.allWorkUnits.keySet());
            result.removeAll(cluster.workUnitMap.keySet());
            result.addAll(cluster.getHandoffWorkUnits());
            result.removeAll(cluster.getHandoffResultWorkUnits());
            result.removeAll(cluster.myWorkUnits);
            return result;
        }
    }

    /**
     * Determines whether or not a given work unit is designated "claimable" by this node.
     * If the ZNode for this work unit is empty, or contains JSON mapping this node to that
     * work unit, it's considered "claimable."
    */
   public boolean isFairGame(String workUnit)
   {
       ObjectNode workUnitData = cluster.allWorkUnits.get(workUnit);
       if (workUnitData == null || workUnitData.size() == 0) {
           return true;
       }

       try {
           JsonNode pegged = workUnitData.get(cluster.name);
           if (pegged == null) {
               return true;
           }
           LOG.debug("Pegged status for {}: {}.", workUnit, pegged);
           return pegged.asText().equals(cluster.myNodeID);
       } catch (Exception e) {
           LOG.error(String.format("Error parsing mapping for %s: %s", workUnit, workUnitData), e);
           return true;
       }
   }

   /**
    * Determines whether or not a given work unit is pegged to this instance.
    */
   public boolean isPeggedToMe(String workUnitId)
   {
       ObjectNode zkWorkData = cluster.allWorkUnits.get(workUnitId);
       if (zkWorkData == null || zkWorkData.size() == 0) {
           cluster.workUnitsPeggedToMe.remove(workUnitId);
           return false;
       }

     try {
         JsonNode pegged = zkWorkData.get(cluster.name);
         final boolean isPegged = (pegged != null) && pegged.asText().equals(cluster.myNodeID);

         if (isPegged) {
             cluster.workUnitsPeggedToMe.add(workUnitId);
         } else {
             cluster.workUnitsPeggedToMe.remove(workUnitId);
         }
         return isPegged;
     } catch (Exception e) {
         LOG.error(String.format("Error parsing mapping for %s: %s", workUnitId, zkWorkData), e);
         return false;
     }
   }

   boolean attemptToClaim(String workUnit)
           throws InterruptedException {
       return attemptToClaim(workUnit, false);
   }
   
   /**
    * Attempts to claim a given work unit by creating an ephemeral node in ZooKeeper
    * with this node's ID. If the claim succeeds, start work. If not, move on.
 * @throws ZooKeeperConnectionException 
 * @throws KeeperException 
    */
   boolean attemptToClaim(String workUnit, boolean claimForHandoff)
       throws InterruptedException
   {
       LOG.debug("Attempting to claim {}. For handoff? {}", workUnit, claimForHandoff);

       String path = claimForHandoff
               ? String.format("/%s/handoff-result/%s", cluster.name, workUnit)
               : cluster.workUnitClaimPath(workUnit);

       final boolean created = ZKUtils.createEphemeral(cluster.zk, path, cluster.myNodeID);

       if (created) {
           if (claimForHandoff) {
               cluster.claimedForHandoff.add(workUnit);
           }
           cluster.startWork(workUnit);
           return true;
       }
       if (isPeggedToMe(workUnit)) {
           claimWorkPeggedToMe(workUnit);
           return true;
       }
       return false;
   }

   /**
    * Claims a work unit pegged to this node, waiting for the ZNode to become available
    * (i.e., deleted by the node which previously owned it).
    */
   protected void claimWorkPeggedToMe(String workUnit)
       throws InterruptedException {
       String path = cluster.workUnitClaimPath(workUnit);

       while (true) {
           if (ZKUtils.createEphemeral(cluster.zk, path, cluster.myNodeID) || cluster.znodeIsMe(path)) {
               cluster.startWork(workUnit);
               return;
           }
           LOG.warn("Attempting to establish ownership of {}. Retrying in one second...", workUnit);
           Thread.sleep(1000);
       }
   }

   protected void drainToCount(int targetCount) {
       drainToCount(targetCount, false);
   }

   public void drainToCount(int targetCount, boolean doShutdown) {
       drainToCount(targetCount, doShutdown, config.useSoftHandoff, null);
   }
   
   /**
    * Drains this node's share of the cluster workload down to a specific number
    * of work units over a period of time specified in the configuration with
    * soft handoff if enabled..
    */
   protected void drainToCount(final int targetCount, final boolean doShutdown, /* Boolean = false */
                    final boolean useHandoff,  /* : Boolean = config.useSoftHandoff */
                    final CountDownLatch latch)
   {
       String msg = useHandoff ? " with handoff" : "";
       LOG.info(String.format("Draining %s%s. Target count: %s, Current: %s",
               config.workUnitName, msg, targetCount, cluster.myWorkUnits.size()));

       if (targetCount >= cluster.myWorkUnits.size()) {
           if (!doShutdown) {
               return;
           }
       } else if (targetCount == 0 && doShutdown) {
           cluster.completeShutdown();
       }

       final int amountToDrain = cluster.myWorkUnits.size() - targetCount;

       String msgPrefix = (useHandoff) ? "Requesting handoff for" : "Shutting down";
       LOG.info("{} {} of {} {} over {} seconds", 
             msgPrefix, amountToDrain, cluster.myWorkUnits.size(), config.workUnitName, config.drainTime);

       // Build a list of work units to hand off.
       LinkedHashSet<String> wuList = new LinkedHashSet<String>(cluster.myWorkUnits);
       wuList.removeAll(cluster.workUnitsPeggedToMe);

       final ArrayList<String> toHandOff = new ArrayList<String>();
       Iterator<String> it = wuList.iterator();
       for (int i = amountToDrain; it.hasNext() && i > 0; --i) {
           toHandOff.add(it.next());
       }

       final int drainInterval = (int) (((double) config.drainTime / (double) toHandOff.size()) * 1000);
       final Iterator<String> drainIt = toHandOff.iterator();

       TimerTask handoffTask = new TimerTask() {
           @Override
           public void run() {
               if (!drainIt.hasNext()) {
                   if (targetCount == 0 && doShutdown)  {
                       cluster.completeShutdown();
                   }
                   if (latch != null) {
                       latch.countDown();
                   }
                   return;
               }
               String workUnit = drainIt.next();
               if (useHandoff && !isPeggedToMe(workUnit)) {
                   try {
                       cluster.requestHandoff(workUnit);
                   } catch (Exception e) {
                       LOG.warn("Problems trying to request handoff of "+workUnit, e);
                   }
               } else {
                   cluster.shutdownWork(workUnit, true);
               }
               cluster.schedule(this, drainInterval, TimeUnit.MILLISECONDS);
           }
       };

       LOG.info("Releasing {} / {} work units over {} seconds: {}",
               amountToDrain, cluster.myWorkUnits.size(), config.drainTime,
               Strings.mkstring(toHandOff, ", "));

       if (!cluster.myWorkUnits.isEmpty()) {
           cluster.schedule(handoffTask, 0, TimeUnit.SECONDS);
       }
   }
}
