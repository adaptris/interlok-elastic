package com.adaptris.core.elastic.rest;

import static org.junit.Assert.assertNotNull;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import com.adaptris.core.http.apache.request.DateHeader;

public class AdvancedElasticRestCreatorTest {

  @SuppressWarnings("deprecation")
  @Test
  public void testCreate() throws Exception {
    AdvancedElasticRestClientCreator creator = new AdvancedElasticRestClientCreator().withRequestInterceptors(new DateHeader());
    TransportClient client = creator.createTransportClient(ElasticRestClientCreatorTest.getTransportUrls());
    assertNotNull(client);
    IOUtils.closeQuietly(client);
  }
}
