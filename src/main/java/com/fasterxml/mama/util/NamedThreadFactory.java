package com.fasterxml.mama.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamedThreadFactory
    implements ThreadFactory
{
    private final String idBase;
    private final int priority;
    private final AtomicInteger nextId = new AtomicInteger(0);

    public NamedThreadFactory(String base) {
        this(base, Thread.NORM_PRIORITY);
    }

    public NamedThreadFactory(String base, int pri) {
        idBase = base;
        priority = pri;
    }
    
    @Override
    public Thread newThread(Runnable r) {
        Thread t = new ErrorLoggingThread(r, nextName());
        t.setPriority(priority);
        return t;
    }

    protected String nextName() {
        return idBase + ":" + nextId.getAndIncrement();
    }
}

class ErrorLoggingThread extends Thread
{
    public ErrorLoggingThread(Runnable r, String name) {
        super(r, name);
    }

    @Override
    public void run() {
        try {
            super.run();
        } catch (Error e) {
            logger().error("Error was thrown in thread '"+getName()+"'", e);
            throw e;
        } catch (RuntimeException e) {
            logger().error("Exception was thrown in thread '"+getName()+"'", e);
            throw e;
        }
    }

    private Logger logger() {
        return LoggerFactory.getLogger(getClass());
    }
}
