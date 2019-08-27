package com.adaptris.core.elastic.rest;

public class PreConfiguredConnection extends ElasticRestConnection {

  private TransportClient client = null;

  public PreConfiguredConnection(TransportClient c) {
    client = c;
  }

  @Override
  public TransportClient getTransport() throws Exception {
    return client;
  }
}
