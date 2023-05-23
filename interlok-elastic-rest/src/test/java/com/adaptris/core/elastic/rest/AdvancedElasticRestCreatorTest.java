package com.adaptris.core.elastic.rest;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.adaptris.core.CoreException;
import com.adaptris.core.http.apache.request.DateHeader;

import interlok.http.apache.async.AsyncWithCredentials;
import interlok.http.apache.async.AsyncWithInterceptors;
import interlok.http.apache.async.CompositeAsyncClientBuilder;
import interlok.http.apache.credentials.AnyScope;
import interlok.http.apache.credentials.DefaultCredentialsProviderBuilder;
import interlok.http.apache.credentials.ScopedCredential;
import interlok.http.apache.credentials.UsernamePassword;

public class AdvancedElasticRestCreatorTest {

  @Test
  public void testCreate_Blank() throws Exception {
    AdvancedElasticRestClientCreator creator = new AdvancedElasticRestClientCreator();
    try (TransportClient client = creator.createTransportClient(ElasticRestClientCreatorTest.getTransportUrls())) {
      assertNotNull(client.restClient());
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreate_WithRequestInterceptors() throws Exception {
    AdvancedElasticRestClientCreator creator = new AdvancedElasticRestClientCreator().withRequestInterceptors(new DateHeader());
    try (TransportClient client = creator.createTransportClient(ElasticRestClientCreatorTest.getTransportUrls())) {
      assertNotNull(client.restClient());
    }
  }

  @Test
  public void testCreate_WithConfig() throws Exception {
    DefaultCredentialsProviderBuilder creds = new DefaultCredentialsProviderBuilder().withScopedCredentials(
        new ScopedCredential().withScope(new AnyScope()).withCredentials(new UsernamePassword().withCredentials("myUser", "myPassword")));
    CompositeAsyncClientBuilder builder = new CompositeAsyncClientBuilder().withBuilders(new AsyncWithCredentials().withProvider(creds),
        new AsyncWithInterceptors().withInterceptors(new DateHeader()));
    AdvancedElasticRestClientCreator creator = new AdvancedElasticRestClientCreator().withAsyncClientBuilderConfig(builder);
    try (TransportClient client = creator.createTransportClient(ElasticRestClientCreatorTest.getTransportUrls())) {
      assertNotNull(client.restClient());
    }
  }

  @Test
  public void testCreate_WithConfig_Exception() throws Exception {
    AdvancedElasticRestClientCreator creator = new AdvancedElasticRestClientCreator().withAsyncClientBuilderConfig(clientBuilder -> {
      throw new CoreException();
    });

    assertThrows(RuntimeException.class, () -> {
      try (TransportClient client = creator.createTransportClient(ElasticRestClientCreatorTest.getTransportUrls())) {
        client.restClient();
      }
    });
  }

}
