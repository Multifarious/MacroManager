package com.fasterxml.slavedriver;

import java.util.concurrent.LinkedBlockingQueue;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

@SuppressWarnings("serial")
public class TokenQueue extends LinkedBlockingQueue<ClaimToken>
{
    public final Meter suppressedMeter;
    public final Meter requestedMeter;

    public TokenQueue(MetricRegistry metrics) {
        suppressedMeter = metrics.meter("slavedriver.suppressedClaimCycles");
        requestedMeter = metrics.meter("slavedriver.claimCycles");
    }

    @Override
    public boolean offer(ClaimToken obj)
    {
        if (contains(obj)) {
            // 15-Oct-2014, tatu: shouldn't semantics suggest 'true' here? But original had it this way...
            suppressedMeter.mark();
            return false;
        }
        requestedMeter.mark();
        return super.offer(obj);
    }
}
