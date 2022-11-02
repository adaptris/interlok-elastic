package com.adaptris.core.elastic.sdk.connection;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.adaptris.core.util.LifecycleHelper;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.autoscaling.ElasticsearchAutoscalingAsyncClient;

public class ElasticConnectionTest {

  private ElasticConnection connection;  
  @Before
  public void setUp() throws Exception {
    connection = new ElasticConnection();
    connection.setTransportUrls(Arrays.asList(new String[] {"http://localhost:9200"}));
    
    LifecycleHelper.prepare(connection);
    LifecycleHelper.init(connection);
  }
  
  @After
  public void tearDown() throws Exception {
    LifecycleHelper.stopAndClose(connection);
  }
  
  @Test
  public void testConnectionStart() throws Exception {
    connection.startConnection();
    
    assertTrue(connection.getClient() instanceof ElasticsearchClient);
  }
  
  @Test
  public void testConnectionStartWithAuthenticator() throws Exception {
    connection.setAuthenticator(new ElasticNoOpAuthenticator());
    connection.startConnection();
    
    assertTrue(connection.getClient() instanceof ElasticsearchClient);
  }
  
  public void testConnectionStartWithBasicAuthenticator() throws Exception {
    ElasticBasicAuthenticator auth = new ElasticBasicAuthenticator();
    auth.setPassword("Password");
    auth.setUsername("Username");
    connection.setAuthenticator(auth);
    connection.startConnection();
    
    assertTrue(connection.getClient() instanceof ElasticsearchClient);
  }
  
  public void testConnectionStartWithApiKeyAuthenticator() throws Exception {
    ElasticApiKeyAuthenticator auth = new ElasticApiKeyAuthenticator();
    auth.setKeyId("KeyID");
    auth.setKeySecret("KeySecret");
    connection.setAuthenticator(auth);
    connection.startConnection();
    
    assertTrue(connection.getClient() instanceof ElasticsearchClient);
  }
}
