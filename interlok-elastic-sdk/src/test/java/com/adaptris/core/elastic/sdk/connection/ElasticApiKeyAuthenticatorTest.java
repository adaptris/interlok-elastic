package com.adaptris.core.elastic.sdk.connection;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.http.Header;
import org.junit.Before;
import org.junit.Test;

public class ElasticApiKeyAuthenticatorTest {

  private static final String API_KEY = "999";
  private static final String API_SECRET = "888";
  private ElasticApiKeyAuthenticator auth;
  
  @Before
  public void setUp() throws Exception {
    auth = new ElasticApiKeyAuthenticator();
  }
  
  @Test
  public void testAuth() {
    auth.setKeyId(API_KEY);
    auth.setKeySecret(API_SECRET);
    Header[] headers = auth.generateDefaultHeaders();
    
    assertEquals(1, headers.length);
    assertTrue(headers[0].getName().equals("Authorization"));
  }
  
  @Test
  public void testAuthBadKEY() {
    auth.setKeyId("PW:");
    auth.setKeySecret("PW:");
    Header[] headers = auth.generateDefaultHeaders();
    
    assertEquals(0, headers.length);
  }
}
