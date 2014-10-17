package com.fasterxml.slavedriver.util;

import java.nio.charset.Charset;
import java.util.ArrayList;

import com.twitter.common.zookeeper.ZooKeeperClient.ZooKeeperConnectionException;
import com.twitter.common.zookeeper.ZooKeeperUtils;
import com.twitter.common.zookeeper.ZooKeeperClient;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKUtils
{
    private final static Logger LOG = LoggerFactory.getLogger(ZKUtils.class);

    private final static Charset UTF8 = Charset.forName("UTF-8");
    private final static byte[] NO_BYTES = new byte[0];
    
    public static void ensureOrdasityPaths(ZooKeeperClient zk, String name, String unit, String unitShort)
            throws InterruptedException
    {
        ArrayList<ACL> acl = Ids.OPEN_ACL_UNSAFE;
        try {
            ZooKeeperUtils.ensurePath(zk, acl, String.format("/%s/nodes", name));
            ZooKeeperUtils.ensurePath(zk, acl, String.format("/%s", unit));
            ZooKeeperUtils.ensurePath(zk, acl, String.format("/%s/meta/rebalance", name));
            ZooKeeperUtils.ensurePath(zk, acl, String.format("/%s/meta/workload", name));
            ZooKeeperUtils.ensurePath(zk, acl, String.format("/%s/claimed-%s", name, unitShort));
            ZooKeeperUtils.ensurePath(zk, acl, String.format("/%s/handoff-requests", name));
            ZooKeeperUtils.ensurePath(zk, acl, String.format("/%s/handoff-result", name));
        } catch (KeeperException e) {
            throw ZKException.from(e);
        } catch (ZooKeeperConnectionException e) {
            throw ZKException.from(e);
        }
    }

    public static boolean createEphemeral(ZooKeeperClient zk, String path)
            throws InterruptedException {
        return createEphemeral(zk, path, NO_BYTES);
    }

    public static boolean createEphemeral(ZooKeeperClient zk, String path, String value)
        throws InterruptedException {
        return createEphemeral(zk, path, utf8BytesFrom(value));
    }

    public static boolean createEphemeral(ZooKeeperClient zk, String path, byte[] value)
            throws InterruptedException
    {
        if (value == null) {
            value = NO_BYTES;
        }
        try {
            zk.get().create(path, value, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            return true;
            
        } catch (NodeExistsException e) {
            return false;
        } catch (KeeperException e) {
            throw ZKException.from(e);
        } catch (ZooKeeperConnectionException e) {
            throw ZKException.from(e);
        }
    }
        
    public static boolean delete(ZooKeeperClient zk, String path)
    {
        try {
            zk.get().delete(path, -1);
            return true;
        } catch (NoNodeException e) {
            LOG.warn("No ZNode to delete for {}", path);
        } catch (Exception e) {
            LOG.error("Unexpected error deleting ZK node {}", path);
        }
        return false;
    }

    /**
     * Attempts to atomically delete the ZNode with the specified path and value. Should be preferred over calling
     * delete() if the value is known.
     *
     * @param zk ZooKeeper client.
     * @param path Path to be deleted.
     * @param expectedValue The expected value of the ZNode at the specified path.
     * @return True if the path was deleted, false otherwise.
     */
    public static boolean deleteAtomic(ZooKeeperClient zk, String path, String expectedValue)
    {
        final Stat stat = new Stat();
        String value = getWithStat(zk, path, stat);
        if (!expectedValue.equals(value)) {
            return false;
        }
        try {
            zk.get().delete(path, stat.getVersion());
            return true;
        } catch (Exception e) {
            LOG.error("Failed to delete path "+path+" with expected value '"+expectedValue+"'", e);
        }
        return false;
    }

    public static boolean set(ZooKeeperClient zk, String path, String data) {
        return set(zk, path, utf8BytesFrom(data));
    }

    public static boolean set(ZooKeeperClient zk, String path, byte[] data)
    {
        try {
            zk.get().setData(path, (data == null) ? NO_BYTES : data, -1);
            return true;
        } catch (Exception e) {
            LOG.error("Error setting "+path, e);
        }
        return false;
    }

    public static void setOrCreate(ZooKeeperClient zk, String path,
            String data, CreateMode mode)
                    throws InterruptedException
    {
        setOrCreate(zk, path, utf8BytesFrom(data), mode);
    }

    public static void setOrCreate(ZooKeeperClient zk, String path,
            byte[] data, CreateMode mode)
        throws InterruptedException
    {
        if (mode == null) {
            mode = CreateMode.EPHEMERAL;
        }
        if (data == null) {
            data = NO_BYTES;
        }
        try {
            zk.get().setData(path, data, -1);
        } catch (NoNodeException e0) {
            try {
                zk.get().create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
            } catch (KeeperException e) {
                throw ZKException.from(e);
            } catch (ZooKeeperConnectionException e) {
                throw ZKException.from(e);
            }
        } catch (KeeperException e) {
            throw ZKException.from(e);
        } catch (ZooKeeperConnectionException e) {
            throw ZKException.from(e);
        }
    }

    public static String get(ZooKeeperClient zk, String path) {
        return getWithStat(zk, path, null);
    }

    public static String getWithStat(ZooKeeperClient zkc, String path, Stat stat)
    {
        try {
            ZooKeeper zk = zkc.get();
            byte[] value = zk.getData(path, false, stat);
            return new String(value, UTF8);
        } catch (NoNodeException e) {
            ; // not considered problematic?
        } catch (Exception e) {
            LOG.error("Error getting data for ZNode at path "+path, e);
        }
        return null;
    }

    public static Stat exists(ZooKeeperClient zk, String path, Watcher watcher) throws InterruptedException
    {
        try {
            return zk.get().exists(path, watcher);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to get stat for ZNode at path {}", path);
        }
        return null;
    }

    public static byte[] utf8BytesFrom(String value) {
        return (value == null) ? NO_BYTES : value.getBytes(UTF8);
    }
}
