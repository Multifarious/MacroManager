package com.fasterxml.mama;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

/*
class Claimer(cluster: Cluster, name: String = "ordasity-claimer") extends Thread(name) {
  private val claimQueue : BlockingQueue[ClaimToken] = new TokenQueue
  def requestClaim() : Boolean = claimQueue.offer(ClaimToken.token)

  override def run() {
    log.info("Claimer started.")
    try {
      while (cluster.getState() != NodeState.Shutdown) {
        claimQueue.take()
        try {
          cluster.claimWork()
        } catch {
          case e: InterruptedException =>
            // Don't swallow these
            throw e
          case e: Exception =>
            log.error("Claimer failed to claim work", e)
        }
      }
    } catch {
      case e: Throwable =>
        log.error("Claimer failed unexpectedly", e)
        throw e
    } finally {
      log.info("Claimer shutting down.")
    }
  }

}
 */
public class Claimer extends Thread
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final BlockingQueue<ClaimToken> claimQueue;
    private final Cluster cluster;
    
    public Claimer(MetricRegistry metrics, Cluster cluster, String name) {
        super(name);
        this.cluster = cluster;
        claimQueue = new TokenQueue(metrics);
    }

    @Override
    public void run() {
        LOG.info("Claimer started.");
        try {
            while (cluster.getState() != NodeState.Shutdown) {
                try {
                    claimQueue.take();
                  cluster.claimWork();
                } catch (InterruptedException e) {
                    // Don't swallow these
                    LOG.warn("Claimer interrupted, exiting");
                    break;
                } catch (Exception e) {
                    LOG.error("Claimer failed to claim work", e);
                }
            }
        } catch (RuntimeException e) {
            LOG.error("Claimer failed unexpectedly", e);
            throw e;
        } finally {
            LOG.info("Claimer shutting down.");
        }
    }

    public Boolean requestClaim() {
        return claimQueue.offer(ClaimToken.token);
    }

}
