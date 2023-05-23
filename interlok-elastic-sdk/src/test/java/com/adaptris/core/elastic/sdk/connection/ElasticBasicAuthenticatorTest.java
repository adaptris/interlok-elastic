package com.adaptris.core.elastic.sdk.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.http.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ElasticBasicAuthenticatorTest {

  private ElasticBasicAuthenticator auth;

  @BeforeEach
  public void setUp() throws Exception {
    auth = new ElasticBasicAuthenticator();
  }

  @AfterEach
  public void tearDown() throws Exception {
  }

  @Test
  public void testGenerateCredentials() {
    auth.setPassword("Password");
    auth.setUsername("Username");

    auth.generateAuthenticator();

    Header[] headers = auth.generateDefaultHeaders();
    assertEquals(0, headers.length);
  }

  public void testGenerateBadPassword() throws Exception {
    auth.setUsername("Username");
    auth.setPassword("PW:");

    auth.generateAuthenticator();

    Header[] headers = auth.generateDefaultHeaders();
    assertEquals(0, headers.length);
  }

}
