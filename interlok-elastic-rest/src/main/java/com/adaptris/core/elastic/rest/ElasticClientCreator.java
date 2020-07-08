package com.adaptris.core.elastic.rest;

import java.util.List;
import org.elasticsearch.client.RestClientBuilder;
import com.adaptris.core.CoreException;

public interface ElasticClientCreator {

  public TransportClient createTransportClient(List<String> transportUrls) throws CoreException;
  
  default RestClientBuilder configure(RestClientBuilder builder) {
    return builder;
  }

}
