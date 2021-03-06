package com.adaptris.core.elastic.rest;

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

public class ElasticRestClientCreatorTest {

  @SuppressWarnings("deprecation")
  @Test
  public void testCreate() throws Exception {
    ElasticRestClientCreator creator = new ElasticRestClientCreator();
    TransportClient client = creator.createTransportClient(getTransportUrls());
    assertNotNull(client);
    IOUtils.closeQuietly(client);
  }

  protected static List<String> getTransportUrls() {
    return Arrays.asList("http://localhost:9200", "localhost:9201", "https://localhost:9202");
  }
}
