package com.fasterxml.mama.twitzk;

import org.apache.zookeeper.ZooKeeper;

/**
   * Encapsulates a user's credentials and has the ability to authenticate them through a
   * {@link ZooKeeper} client.
   */
  public interface ZKCredentials {

    /**
     * A set of {@code Credentials} that performs no authentication.
     */
    ZKCredentials NONE = new ZKCredentials() {
      @Override public void authenticate(ZooKeeper zooKeeper) {
        // noop
      }

      @Override public String scheme() {
        return null;
      }

      @Override public byte[] authToken() {
        return null;
      }
    };

    /**
     * Authenticates these credentials against the given {@code ZooKeeper} client.
     *
     * @param zooKeeper the client to authenticate
     */
    void authenticate(ZooKeeper zooKeeper);

    /**
     * Returns the authentication scheme these credentials are for.
     *
     * @return the scheme these credentials are for or {@code null} if no authentication is
     *     intended.
     */
    String scheme();

    /**
     * Returns the authentication token.
     *
     * @return the authentication token or {@code null} if no authentication is intended.
     */
    byte[] authToken();
  }