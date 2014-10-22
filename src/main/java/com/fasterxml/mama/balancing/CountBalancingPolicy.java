package com.fasterxml.mama.balancing;

import java.util.Set;

import com.fasterxml.mama.Cluster;
import com.fasterxml.mama.listeners.HandoffResultsListener;
import com.fasterxml.mama.util.Strings;

/**
 * Count-based load balancing policy is simple. A node in the cluster
 * will attempt to claim (<n> work units / <k> nodes + 1) work units. It may
 * be initialized with either a simple ClusterListener or a metered SmartListener.
 */
public class CountBalancingPolicy
    extends BalancingPolicy
{
    protected final HandoffResultsListener handoffListener;
    
    public CountBalancingPolicy(Cluster c, HandoffResultsListener l) {
        super(c);
        handoffListener = l;
    }

    /**
     * Claims work in Zookeeper. This method will attempt to divide work about the cluster
     * by claiming up to ((<x> Work Unit Count / <y> Nodes) + 1) work units. While
     * this doesn't necessarily represent an even load distribution based on work unit load,
     * it should result in a relatively even "work unit count" per node. This randomly-distributed
     * amount is in addition to any work units which are pegged to this node.
     */
    @Override
    public void claimWork() throws InterruptedException {
        int claimed = cluster.myWorkUnits.size();
        int nodeCount = activeNodeSize();

        synchronized (cluster.allWorkUnits) {
            final int maxToClaim = getMaxToClaim(nodeCount);

            LOG.debug("{} Nodes: {}. {}: {}.", cluster.name, nodeCount, config.workUnitName, cluster.allWorkUnits.size());
            LOG.debug("Claiming {} pegged to me, and up to {} more.", config.workUnitName, maxToClaim);

            Set<String> unclaimed = getUnclaimed();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Handoff requests: {}, Handoff Results: {}, Unclaimed: {}",
                        cluster.descForHandoffRequests(),
                        cluster.descForHandoffResults(),
                    Strings.mkstring(unclaimed, ", "));
            }

            for (String workUnit : unclaimed) {
                 if ((isFairGame(workUnit) && claimed < maxToClaim) || isPeggedToMe(workUnit)) {
                   if (config.useSoftHandoff && cluster.containsHandoffRequest(workUnit)
                           && attemptToClaim(workUnit, true)) {
                       LOG.info("Accepted handoff of {}.", workUnit);
                       handoffListener.finishHandoff(workUnit);
                       claimed += 1;
                   } else if (!cluster.containsHandoffRequest(workUnit) && attemptToClaim(workUnit)) {
                       claimed += 1;
                   }
                 }
            }
        }
   }

   /**
    * Determines the maximum number of work units the policy should attempt to claim.
    */
   public int getMaxToClaim(int nodeCount) {
       synchronized (cluster.allWorkUnits) {
           final int total = cluster.allWorkUnits.size();
           if (total <= 1) {
               return total;
           }
           return (int) Math.ceil(total / (double) nodeCount);
       }
   }
   

   /**
    * Performs a simple rebalance. Target load is set to (# of work items / node count).
    */
   @Override
   public void rebalance() {
       final int target = fairShare();
       final int mine = cluster.myWorkUnits.size();

       if (mine > target) {
           LOG.info("Simple Rebalance triggered. My Share: {}. Target: {}.", mine, target);
           super.drainToCount(target);
       }
   }

   /**
    * Determines the fair share of work units this node should claim.
    */
   public int fairShare() {
       return (int) Math.ceil((double) cluster.allWorkUnits.size() / (double) activeNodeSize());
   }
}
