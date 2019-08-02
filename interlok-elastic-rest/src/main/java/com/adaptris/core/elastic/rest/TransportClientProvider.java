package com.adaptris.core.elastic.rest;

@FunctionalInterface
public interface TransportClientProvider {

  TransportClient getTransport() throws Exception;
}
