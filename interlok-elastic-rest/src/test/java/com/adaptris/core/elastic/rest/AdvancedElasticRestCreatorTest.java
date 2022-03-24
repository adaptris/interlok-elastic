package com.adaptris.core.elastic.rest;

import static org.junit.Assert.assertNotNull;

import com.adaptris.core.http.apache.request.DateHeader;
import interlok.http.apache.credentials.AnyScope;
import interlok.http.apache.credentials.DefaultCredentialsProviderBuilder;
import interlok.http.apache.credentials.ScopedCredential;
import interlok.http.apache.credentials.UsernamePassword;
import org.junit.Test;

public class AdvancedElasticRestCreatorTest {

  @Test
  public void testCreate_Blank() throws Exception {
    AdvancedElasticRestClientCreator creator = new AdvancedElasticRestClientCreator();
    try (TransportClient client = creator.createTransportClient(ElasticRestClientCreatorTest.getTransportUrls())) {
      assertNotNull(client.restClient());
    }
  }


  @Test
  public void testCreate_WithRequestInterceptors() throws Exception {
    AdvancedElasticRestClientCreator creator = new AdvancedElasticRestClientCreator().withRequestInterceptors(
        new DateHeader());
    try (TransportClient client = creator.createTransportClient(ElasticRestClientCreatorTest.getTransportUrls())) {
      assertNotNull(client.restClient());
    }
  }

  @Test
  public void testCreate_WithCredentialsProvider() throws Exception {
    DefaultCredentialsProviderBuilder credsProvider = new DefaultCredentialsProviderBuilder().withScopedCredentials(
        new ScopedCredential().withScope(new AnyScope())
            .withCredentials(new UsernamePassword().withCredentials("myUser", "myPassword"))
    );
    AdvancedElasticRestClientCreator creator = new AdvancedElasticRestClientCreator().withCredentialsProvider(
        credsProvider);
    try (TransportClient client = creator.createTransportClient(ElasticRestClientCreatorTest.getTransportUrls())) {
      assertNotNull(client.restClient());
    }
  }

}
