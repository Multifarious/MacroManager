package com.fasterxml.slavedriver.balancing;

import java.util.LinkedList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import com.fasterxml.slavedriver.Cluster;

public class MeteredBalancingPolicy
    extends BalancingPolicy
{
    public MeteredBalancingPolicy(Cluster c) {
        super(c);
    }

    /*
    val meters = AtomicMap.atomicNBHM[String, Meter]
            val persistentMeterCache = AtomicMap.atomicNBHM[String, Meter]
            val loadGauge = metrics.gauge[Double]("my_load") { myLoad() }
            var loadFuture : Option[ScheduledFuture[_]] = None

            override def init() : BalancingPolicy = {
              if (!cluster.listener.isInstanceOf[SmartListener]) {
                throw new RuntimeException("Ordasity's metered balancing policy must be initialized with " +
                  "a SmartListener, but you provided a simple listener. Please flip that so we can tick " +
                  "the meter as your application performs work!")
              }

              this
            }
            */

    /**
     * Begins by claimng all work units that are pegged to this node.
     * Then, continues to claim work from the available pool until we've claimed
     * equal to or slightly more than the total desired load.
     */
    @Override
    public void claimWork()
    {
        synchronized (cluster.allWorkUnits) {
            for (String workUnit : getUnclaimed()) {
                if (isPeggedToMe(workUnit)) {
                    claimWorkPeggedToMe(workUnit);
                }
            }
            final double evenD= evenDistribution();

            LinkedList<String> unclaimed = new LinkedList<String>(getUnclaimed());
            while (myLoad() <= evenD && !unclaimed.isEmpty()) {
                final String workUnit = unclaimed.poll();
                if (config.useSoftHandoff && cluster.handoffRequests.contains(workUnit)
                        && isFairGame(workUnit) && attemptToClaim(workUnit, claimForHandoff = true)) {
                    LOG.info(String.format(workUnit));
                    cluster.handoffResultsListener.finishHandoff(workUnit)
                } else if (isFairGame(workUnit)) {
                    attemptToClaim(workUnit);
                }
            }
        }
    }

    /**
     * Performs a "smart rebalance." The target load is set to (cluster load / node count),
     * where "load" is determined by the sum of all work unit meters in the cluster.
     */
    @Override
    public void rebalance() {
        double target = evenDistribution();
        final double myLoad = myLoad();
        if (myLoad > target) {
            LOG.info("Smart Rebalance triggered. Load: %s. Target: %s", myLoad, target);
            drainToLoad(target.longValue);
        }
    }

    /**
     * When smart balancing is enabled, calculates the even distribution of load about
     * the cluster. This is determined by the total load divided by the number of alive nodes.
     */
    public double evenDistribution() {
        return (double) cluster.loadMap.values().sum / (double) activeNodeSize();
    }

    /**
     * Determines the current load on this instance when smart rebalancing is enabled.
     * This load is determined by the sum of all of this node's meters' one minute rate.
     */
    public double myLoad()
    {
        double load = 0d;
        /*
        LOG.debug(cluster.loadMap.toString);
        LOG.debug(cluster.myWorkUnits.toString);
        */
        cluster.myWorkUnits.foreach(u => load += cluster.getOrElse(cluster.loadMap, u, 0));
        return load;
    }

    /**
     * Once a minute, pass off information about the amount of load generated per
     * work unit off to Zookeeper for use in the claiming and rebalancing process.
     */
    private void scheduleLoadTicks() {
        Runnable sendLoadToZookeeper = new Runnable() {
                def run() {
                  try {
                    meters.foreach { case(workUnit, meter) =>
                      val loadPath = "/%s/meta/workload/%s".format(cluster.name, workUnit)
                      ZKUtils.setOrCreate(cluster.zk, loadPath, meter.oneMinuteRate.toString, CreateMode.PERSISTENT)
                    }

                    val myInfo = new NodeInfo(cluster.getState.toString, cluster.zk.get().getSessionId)
                    val nodeLoadPath = "/%s/nodes/%s".format(cluster.name, cluster.myNodeID)
                    val myInfoEncoded = JsonUtils.OBJECT_MAPPER.writeValueAsString(myInfo)
                    ZKUtils.setOrCreate(cluster.zk, nodeLoadPath, myInfoEncoded, CreateMode.EPHEMERAL)

                    LOG.info("My load: %s".format(myLoad()))
                  } catch {
                    case e: Exception => log.error("Error reporting load info to ZooKeeper.", e)
                  }
                }
              }

              loadFuture = Some(cluster.scheduleAtFixedRate(
                sendLoadToZookeeper, 0, 1, TimeUnit.MINUTES))
            }


    /**
     * Drains excess load on this node down to a fraction distributed across the cluster.
     * The target load is set to (clusterLoad / # nodes).
     */
    void drainToLoad(long targetLoad, time: Int = config.drainTime,
            useHandoff: Boolean = config.useSoftHandoff)
    {
        final double startingLoad = myLoad();
        double currentLoad = startingLoad;
        List<String> drainList = new LinkedList<String>();
        val eligibleToDrop = new LinkedList[String](cluster.myWorkUnits -- cluster.workUnitsPeggedToMe);

        while (currentLoad > targetLoad && !eligibleToDrop.isEmpty) {
            val workUnit = eligibleToDrop.poll();
            var workUnitLoad : Double = cluster.getOrElse(cluster.loadMap, workUnit, 0);

            if (workUnitLoad > 0 && (currentLoad - workUnitLoad) > targetLoad) {
                drainList.add(workUnit);
                currentLoad -= workUnitLoad;
            }
        }

        val drainInterval = ((config.drainTime.toDouble / drainList.size) * 1000).intValue();
        val drainTask = buildDrainTask(drainList, drainInterval, useHandoff, currentLoad);

        if (!drainList.isEmpty) {
            LOG.info("Releasing work units over {} seconds. Current load: {}. Target: {}. Releasing: {}",
                    time, startingLoad, targetLoad, drainList.mkString(", "));
            cluster.schedule(drainTask, 0, TimeUnit.SECONDS)
        }
    }

    TimerTask buildDrainTask(final LinkedList<String> drainList, final int drainInterval, final boolean useHandoff,
            final double currentLoad)
    {
        return new TimerTask() {
            @Override
            public void run() {
                  if (drainList.isEmpty() || myLoad() <= evenDistribution) {
                    LOG.info("Finished the drain list, or my load is now less than an even distribution. " +
                      "Stopping rebalance. Remaining work units: %s".format(drainList.mkString(", ")))
                    return;
                  } else if (useHandoff) {
                    cluster.requestHandoff(drainList.poll());
                  } else {
                    cluster.shutdownWork(drainList.poll());
                  }
                  cluster.schedule(this, drainInterval, TimeUnit.MILLISECONDS)
            }
        }
    }

    @Override
    public void onConnect() {
        scheduleLoadTicks()
    }

    @Override
    public void shutdown() {
        if (loadFuture.isDefined) {
            loadFuture.get.cancel(true);
        }
    }

    @Override
    public void onShutdownWork(String workUnit) {
        meters.remove(workUnit);
    }
}
