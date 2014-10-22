package com.fasterxml.mama.balancing;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.mama.*;
import com.fasterxml.mama.listeners.HandoffResultsListener;
import com.fasterxml.mama.util.JsonUtil;
import com.fasterxml.mama.util.Strings;
import com.fasterxml.mama.util.ZKUtils;

public class MeteredBalancingPolicy
    extends BalancingPolicy
{
    protected final Map<String,Meter> meters = new HashMap<String,Meter>();

    // Is this really needed?
    protected final Map<String,Meter> persistentMeterCache = new HashMap<String,Meter>();

    private ScheduledFuture<?> loadFuture;

    private final MetricRegistry metrics;

    protected final HandoffResultsListener handoffListener;
    
    public MeteredBalancingPolicy(Cluster c, HandoffResultsListener l,
            MetricRegistry metrics, SimpleListener listener)
    {
        super(c);
        if (!(listener instanceof SmartListener)) {
                throw new RuntimeException("Ordasity's metered balancing policy must be initialized with " +
                  "a SmartListener, but you provided a simple listener. Please flip that so we can tick " +
                  "the meter as your application performs work!");
        }
        handoffListener = l;
        this.metrics = metrics;

        // 17-Oct-2014, tatu: Not 100% this was correct translation of the intent; would seem
        //   like name could use some sort of prefix but...
        final Gauge<Double> loadGauge = new Gauge<Double>() {
            @Override
            public Double getValue() {
                return myLoad();
            }
        };
        metrics.register("myLoad", loadGauge);
    }

    public Meter findOrCreateMetrics(String workUnit)
    {
        Meter meter;
        synchronized (persistentMeterCache) {
            meter = persistentMeterCache.get(workUnit);
            if (meter == null) {
                meter = new Meter();
                metrics.register(workUnit+".processing", meter);
            }
        }
        synchronized (meters) {
            meters.put(workUnit, meter);
        }
        return meter;
    }
    
    /**
     * Begins by claiming all work units that are pegged to this node.
     * Then, continues to claim work from the available pool until we've claimed
     * equal to or slightly more than the total desired load.
     */
    @Override
    public void claimWork() throws InterruptedException
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
                if (config.useSoftHandoff && cluster.containsHandoffRequest(workUnit)
                        && isFairGame(workUnit) && attemptToClaim(workUnit, true)) {
                    LOG.info(String.format(workUnit));
                    handoffListener.finishHandoff(workUnit);
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
            drainToLoad((long) target);
        }
    }

    /**
     * When smart balancing is enabled, calculates the even distribution of load about
     * the cluster. This is determined by the total load divided by the number of alive nodes.
     */
    public double evenDistribution() {
        return cluster.getTotalWorkUnitLoad() / (double) activeNodeSize();
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
        for (String wu : cluster.myWorkUnits) {
            Double d = cluster.getWorkUnitLoad(wu);
            if (d != null) {
                load += d.doubleValue();
            }
        }
        return load;
    }

    /**
     * Once a minute, pass off information about the amount of load generated per
     * work unit off to Zookeeper for use in the claiming and rebalancing process.
     */
    private void scheduleLoadTicks() {
        Runnable sendLoadToZookeeper = new Runnable() {
            @Override
            public void run() {
                final List<String> loads = new ArrayList<String>();
                synchronized (meters) {
                    for (Map.Entry<String,Meter> entry : meters.entrySet()) {
                        final String workUnit = entry.getKey();
                        loads.add(String.format("/%s/meta/workload/%s", cluster.name, workUnit));
                        loads.add(String.valueOf(entry.getValue().getOneMinuteRate()));
                    }
                }

                Iterator<String> it = loads.iterator();
                while (it.hasNext()) {
                    final String path = it.next();
                    final String rate = it.next();
                    try {
                        ZKUtils.setOrCreate(cluster.zk, path, rate, CreateMode.PERSISTENT);
                    } catch (Exception e) {
                        // should we fail the loop too?
                        LOG.error("Problems trying to store load rate for {} (value {}): ({}) {}",
                                path, rate, e.getClass().getName(), e.getMessage());
                    }
                }

                String nodeLoadPath = String.format("/%s/nodes/%s", cluster.name, cluster.myNodeID);
                try {
                    NodeInfo myInfo = new NodeInfo(cluster.getState().toString(), cluster.zk.get().getSessionId());
                    byte[] myInfoEncoded = JsonUtil.asJSONBytes(myInfo);
                    ZKUtils.setOrCreate(cluster.zk, nodeLoadPath, myInfoEncoded, CreateMode.EPHEMERAL);
                    LOG.info("My load: {}", myLoad());
                } catch (Exception e) {
                    LOG.error("Error reporting load info to ZooKeeper.", e);
                }
            }
        };

        loadFuture = cluster.scheduleAtFixedRate(sendLoadToZookeeper, 0, 1, TimeUnit.MINUTES);
    }

    protected void drainToLoad(long targetLoad) {
        drainToLoad(targetLoad, config.drainTime, config.useSoftHandoff);
    }
    
    /**
     * Drains excess load on this node down to a fraction distributed across the cluster.
     * The target load is set to (clusterLoad / # nodes).
     */
    protected void drainToLoad(long targetLoad, int time, boolean useHandoff)
    {
        final double startingLoad = myLoad();
        double currentLoad = startingLoad;
        List<String> drainList = new LinkedList<String>();
        Set<String> eligibleToDrop = new LinkedHashSet<String>(cluster.myWorkUnits);
        eligibleToDrop.removeAll(cluster.workUnitsPeggedToMe);

        for (String workUnit : eligibleToDrop) {
            if (currentLoad <= targetLoad) {
                break;
            }
            double workUnitLoad = cluster.getWorkUnitLoad(workUnit);

            if (workUnitLoad > 0 && (currentLoad - workUnitLoad) > targetLoad) {
                drainList.add(workUnit);
                currentLoad -= workUnitLoad;
            }
        }

        int drainInterval = (int) (((double) config.drainTime / drainList.size()) * 1000);
        TimerTask drainTask = buildDrainTask(drainList, drainInterval, useHandoff, currentLoad);

        if (!drainList.isEmpty()) {
            LOG.info("Releasing work units over {} seconds. Current load: {}. Target: {}. Releasing: {}",
                    time, startingLoad, targetLoad, Strings.mkstring(drainList, ", "));
            cluster.schedule(drainTask, 0, TimeUnit.SECONDS);
        }
    }

    TimerTask buildDrainTask(final List<String> drainList, final int drainInterval,
            final boolean useHandoff, final double currentLoad)
    {
        final Iterator<String> it = drainList.iterator();
        return new TimerTask() {
            @Override
            public void run() {
                  if (!it.hasNext() || myLoad() <= evenDistribution()) {
                      LOG.info("Finished the drain list, or my load is now less than an even distribution. " +
                              "Stopping rebalance. Remaining work units: {}",
                              Strings.mkstring(drainList, ", "));
                    return;
                  }
                  String workUnit = it.next();
                  if (useHandoff) {
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
    }

    @Override
    public void onConnect() {
        scheduleLoadTicks();
    }

    @Override
    public void shutdown() {
        if (loadFuture != null) {
            loadFuture.cancel(true);
        }
    }

    @Override
    public void onShutdownWork(String workUnit) {
        synchronized (meters) {
            meters.remove(workUnit);
        }
    }
}
