package com.adaptris.core.elastic.rest;

import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import com.adaptris.core.CoreException;

public class ElasticRestClientCreator implements ElasticClientCreator {

  @Override
  public TransportClient createTransportClient(List<String> transportUrls) throws CoreException {
    List<HttpHost> hosts = new ArrayList<>();
    for(String transportUrl : transportUrls) {
      hosts.add(HttpHost.create(transportUrl));
    }
    RestClientBuilder restClientBuilder = RestClient.builder(hosts.toArray(new HttpHost[0]));
    RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
    Sniffer sniffer = Sniffer.builder(client.getLowLevelClient()).build();
    return new TransportClient(client, sniffer);
  }

}
