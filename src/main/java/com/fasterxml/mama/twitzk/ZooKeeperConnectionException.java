package com.fasterxml.mama.twitzk;

/**
 * Indicates an error connecting to a zookeeper cluster.
 */
@SuppressWarnings("serial")
public class ZooKeeperConnectionException extends Exception {
    public ZooKeeperConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
