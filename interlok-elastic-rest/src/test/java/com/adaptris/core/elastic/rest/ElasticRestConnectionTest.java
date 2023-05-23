package com.adaptris.core.elastic.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import com.adaptris.core.util.LifecycleHelper;

public class ElasticRestConnectionTest {

  @Test
  public void testLifecycle() throws Exception {
    ElasticRestConnection conn = new ElasticRestConnection("http://localhost:9200")
        .withElasticClientCreator(new ElasticRestClientCreator());
    try {
      LifecycleHelper.initAndStart(conn);
    } finally {
      LifecycleHelper.stopAndClose(conn);
    }
  }

  @Test
  public void testGetTransport() throws Exception {
    ElasticRestConnection conn = new ElasticRestConnection("http://localhost:9200");
    try {
      LifecycleHelper.initAndStart(conn);
      TransportClient client = conn.getTransport();
      assertNotNull(conn.getTransport());
      assertEquals(client, conn.getTransport());
    } finally {
      LifecycleHelper.stopAndClose(conn);
    }

  }
}
