package com.fasterxml.mama;

import com.codahale.metrics.Meter;

public abstract class SmartListener extends SimpleListener {
    public void startWork(String workUnit, Meter meter) { }
}