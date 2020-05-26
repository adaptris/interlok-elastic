package com.adaptris.core.elastic.rest;

import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import com.adaptris.core.CoreException;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

@XStreamAlias("default-elastic-rest-client")
@NoArgsConstructor
public class ElasticRestClientCreator implements ElasticClientCreator {

  @Override
  public TransportClient createTransportClient(List<String> transportUrls) throws CoreException {
    List<HttpHost> hosts = new ArrayList<>();
    transportUrls.forEach((url) -> hosts.add(HttpHost.create(url)));
    RestClientBuilder restClientBuilder = configure(RestClient.builder(hosts.toArray(new HttpHost[0])));
    RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
    Sniffer sniffer = Sniffer.builder(client.getLowLevelClient()).build();
    return new TransportClient(client, sniffer);
  }

}
