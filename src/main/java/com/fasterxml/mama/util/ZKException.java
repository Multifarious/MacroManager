package com.fasterxml.mama.util;

import org.apache.zookeeper.KeeperException;

import com.twitter.common.zookeeper.ZooKeeperClient.ZooKeeperConnectionException;

public class ZKException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    public ZKException(KeeperException e) {
        super(e.getMessage(), e);
    }

    public ZKException(ZooKeeperConnectionException e) {
        super(e.getMessage(), e);
    }

    public static ZKException from(KeeperException e) { return new ZKException(e); }
    public static ZKException from(ZooKeeperConnectionException e) { return new ZKException(e); }
}
