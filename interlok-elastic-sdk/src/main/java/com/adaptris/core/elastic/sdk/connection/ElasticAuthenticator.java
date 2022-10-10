package com.adaptris.core.elastic.sdk.connection;

import org.apache.http.Header;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticAuthenticator {
  
  protected transient Logger log = LoggerFactory.getLogger(this.getClass().getName());

  protected HttpClientConfigCallback generateAuthenticator() {
    
    return new HttpClientConfigCallback() {
      @Override
      public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
          return httpClientBuilder;
      }
    };
  }
  
  protected Header[] generateDefaultHeaders() {
    return new Header[0];
  }

}
