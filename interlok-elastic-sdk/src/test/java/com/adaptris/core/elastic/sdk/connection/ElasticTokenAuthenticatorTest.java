package com.adaptris.core.elastic.sdk.connection;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.http.Header;
import org.junit.Before;
import org.junit.Test;

public class ElasticTokenAuthenticatorTest {

  private static final String TOKEN = "999";
  private ElasticTokenAuthenticator auth;
  
  @Before
  public void setUp() throws Exception {
    auth = new ElasticTokenAuthenticator();
  }
  
  @Test
  public void testAuth() {
    auth.setToken(TOKEN);
    Header[] headers = auth.generateDefaultHeaders();
    
    assertEquals(1, headers.length);
    assertTrue(headers[0].getName().equals("Authorization"));
    assertTrue(headers[0].getValue().contains(TOKEN));
  }
  
  @Test
  public void testAuthNoToken() {
    auth.setToken("PW:");
    Header[] headers = auth.generateDefaultHeaders();
    
    assertEquals(0, headers.length);
  }
}
