package com.adaptris.core.elastic.sdk.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.http.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ElasticApiKeyAuthenticatorTest {

  private static final String API_KEY = "999";
  private static final String API_SECRET = "888";
  private ElasticApiKeyAuthenticator auth;

  @BeforeEach
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
