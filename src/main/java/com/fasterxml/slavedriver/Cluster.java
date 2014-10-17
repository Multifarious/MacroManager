package com.fasterxml.slavedriver;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.lang.management.ManagementFactory;

import javax.management.ObjectName;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.slavedriver.balancing.BalancingPolicy;
import com.fasterxml.slavedriver.balancing.CountBalancingPolicy;
import com.fasterxml.slavedriver.balancing.MeteredBalancingPolicy;
import com.fasterxml.slavedriver.listeners.ClusterNodesChangedListener;
import com.fasterxml.slavedriver.listeners.HandoffResultsListener;
import com.fasterxml.slavedriver.listeners.VerifyIntegrityListener;
import com.fasterxml.slavedriver.util.JsonUtil;
import com.fasterxml.slavedriver.util.NamedThreadFactory;
import com.fasterxml.slavedriver.util.Strings;
import com.fasterxml.slavedriver.util.ZKUtils;
import com.twitter.common.quantity.Time;
import com.twitter.common.quantity.Amount;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperMap;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster
    implements ClusterMBean
    //with Instrumented
{
    final private Logger LOG = LoggerFactory.getLogger(getClass());

    // Register with JMX for management / instrumentation.
    /*
    ManagementFactory.getPlatformMBeanServer()
        .registerMBean(this, new ObjectName(name + ":" + "name=Cluster"));
        */
    
    final public String name;
    final public String myNodeID;

    final private String shortName;
    final private SimpleListener listener;
    final private ClusterConfig config;
    
    final private AtomicBoolean watchesRegistered = new AtomicBoolean(false);
    final private AtomicBoolean initialized = new AtomicBoolean(false);
    final private CountDownLatch initializedLatch = new CountDownLatch(1);
    final private AtomicBoolean connected = new AtomicBoolean(false);

    // Cluster, node, and work unit state
    public Map<String,NodeInfo> nodes;
    public final Set<String> myWorkUnits = new NonBlockingHashSet<String>();
    public Map<String,ObjectNode> allWorkUnits;
    public Map<String,String> workUnitMap;
    public Map<String,String> handoffRequests;
    public Map<String,String> handoffResults;
    public Set<String> claimedForHandoff = new NonBlockingHashSet<String>();
    public Map<String,Double> loadMap = Collections.emptyMap();
    public Set<String> workUnitsPeggedToMe = new NonBlockingHashSet<String>();
    final private Claimer claimer;

    public final AtomicReference<NodeState> state = new AtomicReference<NodeState>(NodeState.Fresh);
    
    public final HandoffResultsListener handoffResultsListener;

    final private BalancingPolicy balancingPolicy;

    // Scheduled executions
    private final AtomicReference<ScheduledThreadPoolExecutor> pool;
    private ScheduledFuture<?> autoRebalanceFuture; // Option[ScheduledFuture[_]] = None

    // Metrics

    final private Gauge<String> listGauge;
    final private Gauge<Integer> countGauge;
    final private Gauge<String> connStateGauge;
    final private Gauge<String> nodeStateGauge;

    // And ZooKeeper
            
    public ZooKeeperClient zk;

    public Cluster(String n, SimpleListener l, ClusterConfig config) {
        this(n, l, config, new MetricRegistry());
    }
    
    public Cluster(String n, SimpleListener l, ClusterConfig config,
            MetricRegistry metrics)
    {
        name = n;
        listener = l;
        this.config = config;
        myNodeID = config.nodeId;
        shortName = config.workUnitShortName;

        claimer = new Claimer(metrics, this, "ordasity-claimer-" + name);
        handoffResultsListener = new HandoffResultsListener(this);
        balancingPolicy = config.useSmartBalancing
                ? new MeteredBalancingPolicy(this).init()
                : new CountBalancingPolicy(this).init()
                ;
        pool = new AtomicReference<ScheduledThreadPoolExecutor>(createScheduledThreadExecutor());
        
        listGauge = new Gauge<String>() {
            @Override
            public String getValue() {
                return Strings.mkstring(myWorkUnits, ", ");
            };
        };
        metrics.register("my_" + shortName,listGauge);
        countGauge = new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return myWorkUnits.size();
            };
        };
        metrics.register("my_" + shortName + "_count", countGauge);
        connStateGauge = new Gauge<String>() {
            @Override
            public String getValue() {
                return connected.get() ? "true" : "false";
            };
        };
        metrics.register("zk_connection_state", connStateGauge);
        nodeStateGauge = new Gauge<String>() {
            @Override
            public String getValue() {
                return getState().toString();
            };
        };
        metrics.register("node_state", nodeStateGauge);
    
    }

    private ScheduledThreadPoolExecutor createScheduledThreadExecutor() {
        return new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("ordasity-scheduler"));
    }
    
    // // // Trivial accessors

    public boolean isInitialized() {
        return initialized.get();
    }

    public boolean isMe(String other) {
        return myNodeID.equals(other);
    }
    
    /**
     * Given a path, determines whether or not the value of a ZNode is my node ID.
     */
    public boolean znodeIsMe(String path) {
        String value = ZKUtils.get(zk, path);
        return (value != null && value == myNodeID);
    }
    
    public ClusterConfig getConfig() { return config; }

    public NodeState getState() {
        return state.get();
    }

    public boolean hasState(NodeState s) {
        return state.get() == s;
    }
    
    // // // Access to helper objects
    
    public void schedule(Runnable r, long delay, TimeUnit unit) {
        pool.get().schedule(r, delay, unit);
    }

    public void scheduleAtFixedRate(Runnable r, long initial, long period, TimeUnit unit) {
        pool.get().scheduleAtFixedRate(r, initial, period, unit);
    }
    
    // // // Active API
    
    /**
     * Joins the cluster, claims work, and begins operation.
     */
    @Override
    public String join() {
        return join(null);
    }

    /**
     * Joins the cluster using a custom zk client, claims work, and begins operation.
     */
    public String join(ZooKeeperClient injectedClient)
    {
        switch (state.get()) {
        case Fresh:
            connect(injectedClient);
            break;
        case Shutdown:
            connect(injectedClient);
            break;
        case Draining:
            LOG.warn("'join' called while draining; ignoring.");
            break;
        case Started:
            LOG.warn("'join' called after started; ignoring.");
            break;
        }
        // why note Enum itself?
        return state.get().toString();
    }

    /**
     * registers a shutdown hook which causes cleanup of ephemeral state in zookeeper
     * when the JVM exits normally (via Ctrl+C or SIGTERM for example)
     *
     * this alerts other applications which have discovered this instance that it is
     * down so they may avoid remitting requests. otherwise this will not happen until
     * the default zookeeper timeout of 10s during which requests will fail until
     * the application is up and accepting requests again
     */
    protected void addShutdownHook() {
      Runtime.getRuntime().addShutdownHook(
        new Thread() {
            @Override
            public void run() {
                LOG.info("Cleaning up ephemeral ZooKeeper state");
                completeShutdown();
            }
        });
    }

    final private Watcher connectionWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            final KeeperState ks = event.getState();
            switch (ks) {
            case SyncConnected:
                LOG.info("ZooKeeper session established.");
                connected.set(true);
                try {
                    if (state.get() != NodeState.Shutdown) {
                        onConnect();
                    } else {
                        LOG.info("This node is shut down. ZK connection re-established, but not relaunching.");
                    }
                } catch (Exception e) {
                    LOG.error("Exception during zookeeper connection established callback", e);
                }
                break;
            case Expired:
                LOG.info("ZooKeeper session expired.");
                connected.set(false);
                forceShutdown();
                awaitReconnect();
            case Disconnected:
                LOG.info("ZooKeeper session disconnected. Awaiting reconnect...");
                connected.set(false);
                awaitReconnect();

            default: // actually, should only be AuthFaiLed?
                LOG.info("ZooKeeper session interrupted. Shutting down due to event "+ks);
                connected.set(false);
                awaitReconnect();
            }
        }
    };

    private void awaitReconnect() {
        while (true) {
          try {
              LOG.info("Awaiting reconnection to ZooKeeper...");
              zk.get(Amount.of(1L, Time.SECONDS));
          } catch (TimeoutException e) {
              LOG.warn("Timed out reconnecting to ZooKeeper.", e);
          } catch (Exception e) {
              LOG.error("Error reconnecting to ZooKeeper", e);
          }
        }
    }

    /**
     * Directs the ZooKeeperClient to connect to the ZooKeeper ensemble and wait for
     * the connection to be established before continuing.
     */
    private void connect(ZooKeeperClient injectedClient) {
        if (!initialized.get()) {
            val hosts = config.hosts.split(",").map { server =>
              val host = server.split(":")(0)
              val port = Integer.parseInt(server.split(":")(1))
              new InetSocketAddress(host, port)
            }.toList();
    
            claimer.start();
            LOG.info("Connecting to hosts: {}", hosts.toString());
            if (injectedClient == null) {
                injectedClient = new ZooKeeperClient(Amount.of(config.zkTimeout, Time.MILLISECONDS), hosts);
            }
            zk = injectedClient;
            LOG.info("Registering connection watcher.");
            zk.register(connectionWatcher);
        }
        zk.get();
    }

    /**
     * Drains all work claimed by this node over the time period provided in the config
     * (default: 60 seconds), prevents it from claiming new work, and exits the cluster.
     */
    @Override
    public void shutdown() {
        if (state.get() == NodeState.Shutdown) {
            return;
        }
        balancingPolicy.shutdown();
        if (autoRebalanceFuture != null) {
            autoRebalanceFuture.cancel(true);
        }
        LOG.info("Shutdown initiated; beginning drain...");
        setState(NodeState.Draining);
        balancingPolicy.drainToCount(0, true);
    }

    private void forceShutdown() {
        balancingPolicy.shutdown();
        if (autoRebalanceFuture.isDefined()) {
            autoRebalanceFuture.get().cancel(true);
        }
        LOG.warn("Forcible shutdown initiated due to connection loss...");
        myWorkUnits.map(w => shutdownWork(w));
        myWorkUnits.clear();
        listener.onLeave();
    }

    /**
     * Finalizes the shutdown sequence. Called once the drain operation completes.
     */
    public void completeShutdown() {
      setState(NodeState.Shutdown);
      myWorkUnits.map(w => shutdownWork(w))
      myWorkUnits.clear();
      deleteFromZk();
      if (claimer != null) {
        claimer.interrupt();
        claimer.join();
      }
      // The connection watcher will attempt to reconnect - unregister it
      if (connectionWatcher != null) {
        zk.unregister(connectionWatcher);
      }
      try {
        zk.close();
      } catch (Exception e) {
          LOG.warn("Zookeeper reported exception on shutdown.", e);
      }
      listener.onLeave();
    }

    /**
     * remove this worker's ephemeral node from zk
     */
    private void deleteFromZk() {
        ZKUtils.delete(zk, "/" + name + "/nodes/" + myNodeID);
    }

    /**
     * Primary callback which is triggered upon successful Zookeeper connection.
     */
    private void onConnect() {
      if (state.get() != NodeState.Fresh) {
        if (previousZKSessionStillActive()) {
            LOG.info("ZooKeeper session re-established before timeout.");
          return;
        }
        LOG.warn("Rejoined after session timeout. Forcing shutdown and clean startup.");
        ensureCleanStartup();
      }

      LOG.info("Connected to Zookeeper (ID: {}).", myNodeID);
      ZKUtils.ensureOrdasityPaths(zk, name, config.workUnitName, config.workUnitShortName);

      joinCluster();

      listener.onJoin(zk);

      if (watchesRegistered.compareAndSet(false, true)) {
        registerWatchers();
      }
      initialized.set(true);
      initializedLatch.countDown();
      
      setState(NodeState.Started);
      claimer.requestClaim();
      verifyIntegrity();

      balancingPolicy.onConnect();
      if (config.enableAutoRebalance) {
          scheduleRebalancing();
      }
    }

    /**
     * In the event that the node has been evicted and is reconnecting, this method
     * clears out all existing state before relaunching to ensure a clean launch.
     */
    private void ensureCleanStartup() {
        forceShutdown();
        ScheduledThreadPoolExecutor oldPool = pool.getAndSet(createScheduledThreadExecutor());
        oldPool.shutdownNow();
        myWorkUnits.map(w => shutdownWork(w));
        myWorkUnits.clear();
        claimedForHandoff.clear();
        workUnitsPeggedToMe.clear();
        state.set(NodeState.Fresh);
    }

    /**
     * Schedules auto-rebalancing if auto-rebalancing is enabled. The task is
     * scheduled to run every 60 seconds by default, or according to the config.
     */
    private void scheduleRebalancing() {
      int interval = config.autoRebalanceInterval;
      Runnable runRebalance = new Runnable() {
          @Override
          public void run() {
              try {
                  rebalance();
              } catch (Exception e) {
                  LOG.error("Error running auto-rebalance.", e);
              }
          }
      };

      autoRebalanceFuture = pool.get().scheduleAtFixedRate(runRebalance, interval, interval, TimeUnit.SECONDS);
    }


    /**
     * Registers this node with Zookeeper on startup, retrying until it succeeds.
     * This retry logic is important in that a node which restarts before Zookeeper
     * detects the previous disconnect could prohibit the node from properly launching.
     */
    private void joinCluster() {
      while (true) {
          NodeInfo myInfo = new NodeInfo(NodeState.Fresh.toString(), zk.get().getSessionId());
          byte[] encoded = JsonUtil.asJSONBytes(myInfo);
          if (ZKUtils.createEphemeral(zk, "/" + name + "/nodes/" + myNodeID, encoded)) {
              return;
          }
          LOG.warn("Unable to register with Zookeeper on launch. " +
                  "Is {} already running on this host? Retrying in 1 second...", name);
          Thread.sleep(1000);
      }
    }

    /**
     * Registers each of the watchers that we're interested in in Zookeeper, and callbacks.
     * This includes watchers for changes to cluster topology (/nodes), work units
     * (/work-units), and claimed work (/<service-name>/claimed-work). We also register
     * watchers for calls to "/meta/rebalance", and if smart balancing is enabled, we'll
     * watch "<service-name>/meta/workload" for changes to the cluster's workload.
     */
    private void registerWatchers()
    {
        ClusterNodesChangedListener nodesChangedListener = new ClusterNodesChangedListener(this);
        VerifyIntegrityListenerr<String> verifyIntegrityListener =
                new VerifyIntegrityListener<String>(this, config);
        StringDeserializer stringDeser = new StringDeserializer();

        nodes = ZooKeeperMap.create(zk, String.format("/%s/nodes", name),
                new NodeInfoDeserializer(), nodesChangedListener);

        allWorkUnits = ZooKeeperMap.create(zk, String.format("/%s", config.workUnitName),
                new ObjectNodeDeserializer, new VerifyIntegrityListener<ObjectNode>(this, config));

        workUnitMap = ZooKeeperMap.create(zk, String.format("/%s/claimed-%s", name, config.workUnitShortName),
                stringDeser, verifyIntegrityListener);

        // Watch handoff requests and results.
        if (config.useSoftHandoff) {
            handoffRequests = ZooKeeperMap.create(zk, String.format("/%s/handoff-requests", name),
                    stringDeser, verifyIntegrityListener);

            handoffResults = ZooKeeperMap.create(zk, String.format("/%s/handoff-result", name),
                    stringDeser, handoffResultsListener);
        } else {
            handoffRequests = new HashMap<String, String>();
            handoffResults = new HashMap<String, String>();
        }

        // If smart balancing is enabled, watch for changes to the cluster's workload.
        if (config.useSmartBalancing) {
            loadMap = ZooKeeperMap.<Double>create(zk, String.format("/%s/meta/workload", name),
                    new DoubleDeserializer());
        }
    }


    /**
     * Triggers a work-claiming cycle. If smart balancing is enabled, claim work based
     * on node and cluster load. If simple balancing is in effect, claim by count.
     */
    public void claimWork() {
        if (state.get() == NodeState.Started && connected.get()) {
            balancingPolicy.claimWork();
        }
    }

    public void requestClaim() {
        claimer.requestClaim();
    }
    
    /**
      * Requests that another node take over for a work unit by creating a ZNode
      * at handoff-requests. This will trigger a claim cycle and adoption.
     */
    public void requestHandoff(String workUnit) {
        LOG.info("Requesting handoff for {}.", workUnit);
        ZKUtils.createEphemeral(zk, "/" + name + "/handoff-requests/" + workUnit);
    }

    /**
     * Verifies that all nodes are hooked up properly. Shuts down any work units
     * which have been removed from the cluster or have been assigned to another node.
     */
    public void verifyIntegrity()
    {
        val noLongerActive = myWorkUnits -- allWorkUnits.keys.toSet;
        for (workUnit <- noLongerActive) {
            shutdownWork(workUnit);
        }

        // Check the status of pegged work units to ensure that this node is not serving
        // a work unit that is pegged to another node in the cluster.
        myWorkUnits.map { workUnit =>
        val claimPath = workUnitClaimPath(workUnit)
        if (!balancingPolicy.isFairGame(workUnit) && !balancingPolicy.isPeggedToMe(workUnit)) {
          LOG.info("Discovered I'm serving a work unit that's now " +
            "pegged to someone else. Shutting down %s".format(workUnit))
          shutdownWork(workUnit);

        } else if (workUnitMap.contains(workUnit) && !workUnitMap.get(workUnit).equals(myNodeID) &&
            !claimedForHandoff.contains(workUnit) && !znodeIsMe(claimPath)) {
            LOG.info("Discovered I'm serving a work unit that's now " +
                    "claimed by {} according to ZooKeeper. Shutting down {}",
                    workUnitMap.get(workUnit), workUnit);
            shutdownWork(workUnit);
        }
      }
    }

    public String workUnitClaimPath(String workUnit) {
        return String.format("/%s/claimed-%s/%s", name, config.workUnitShortName, workUnit);
    }

    /**
     * Starts up a work unit that this node has claimed.
     * If "smart rebalancing" is enabled, hand the listener a meter to mark load.
     * Otherwise, just call "startWork" on the listener and let the client have at it.
     * TODO: Refactor to remove check and cast.
     */
    public void startWork(String workUnit, Meter meter) {
        LOG.info("Successfully claimed {}: {}. Starting...", config.workUnitName, workUnit);
        final boolean added = myWorkUnits.add(workUnit);

        if (added) {
            if (balancingPolicy instanceof MeteredBalancingPolicy) {
                MeteredBalancingPolicy mbp = (MeteredBalancingPolicy) balancingPolicy;
                val meter = mbp.persistentMeterCache.getOrElseUpdate(
                        workUnit, metrics.meter(workUnit, "processing"));
                mbp.meters.put(workUnit, meter);
                ((SmartListener) listener).startWork(workUnit, meter);
            } else {
                ((ClusterListener) listener).startWork(workUnit);
            }
        } else {
            LOG.warn("Detected that %s is already a member of my work units; not starting twice!", workUnit);
        }
    }

    /**
     * Shuts down a work unit by removing the claim in ZK and calling the listener.
     */
    public boolean shutdownWork(String workUnit, boolean doLog /*true*/ ) {
        if (doLog) {
            LOG.info("Shutting down {}: {}...", config.workUnitName, workUnit);
        }
        myWorkUnits.remove(workUnit);
        claimedForHandoff.remove(workUnit);
        balancingPolicy.onShutdownWork(workUnit);
        try {
            listener.shutdownWork(workUnit);
        } finally {
            ZKUtils.deleteAtomic(zk, workUnitClaimPath(workUnit), myNodeID);
        }
    }


    /**
     * Initiates a cluster rebalance. If smart balancing is enabled, the target load
     * is set to (total cluster load / node count), where "load" is determined by the
     * sum of all work unit meters in the cluster. If smart balancing is disabled,
     * the target load is set to (# of work items / node count).
     */
    @Override
    public void rebalance() {
        if (state.get() != NodeState.Fresh) {
            balancingPolicy.rebalance();
        }
    }

    /**
     * Sets the state of the current Ordasity node and notifies others via ZooKeeper.
    */
    private void setState(NodeState to) {
      final NodeInfo myInfo = new NodeInfo(to.toString(), zk.get().getSessionId());
      byte[] encoded = JsonUtil.asJSONBytes(myInfo);
      ZKUtils.set(zk, "/" + name + "/nodes/" + myNodeID, encoded);
      state.set(to);
    }


    /**
     * Determines if another ZooKeeper session is currently active for the current node
     * by comparing the ZooKeeper session ID of the connection stored in NodeState.
    */
    private boolean previousZKSessionStillActive()
    {
        try {
            byte[] json = zk.get().getData(String.format("/%s/nodes/%s", name, myNodeID), false, null);
            NodeInfo nodeInfo = JsonUtil.fromJSON(json, NodeInfo.class);
            return (nodeInfo.connectionID == zk.get().getSessionId());
        } catch (NoNodeException e) {
            ; // apparently harmless?
        } catch (Exception e) {
            LOG.error("Encountered unexpected error in checking ZK session status.", e);
        }
        return false;
    }
}
