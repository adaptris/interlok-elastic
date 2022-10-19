package com.adaptris.core.elastic.sdk.connection;

import java.util.ArrayList;
import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisConnectionImp;
import com.adaptris.core.CoreException;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@XStreamAlias("elastic-sdk-connection")
@AdapterComponent
@ComponentProfile(summary = "Connection to your Elasticsearch version 7 server.", tag = "connections,elastic,elasticsearch")
public class ElasticConnection extends AdaptrisConnectionImp {

  @Getter
  @Setter
  private transient ElasticsearchClient client;
  /**
   * The list of URLs that we try to connect to.
   * 
   */
  @XStreamImplicit(itemFieldName = "transport-url")
  @Size(min = 1)
  @Valid
  @Getter
  @Setter
  @NotNull
  @NonNull
  private List<String> transportUrls;
  @Getter
  @Setter
  private ElasticAuthenticator authenticator;
  
  @Override
  protected void prepareConnection() throws CoreException {
    
  }

  @Override
  protected void initConnection() throws CoreException {
    
  }

  @Override
  protected void startConnection() throws CoreException {
    List<HttpHost> hosts = new ArrayList<>();
    transportUrls.forEach((url) -> hosts.add(HttpHost.create(url)));

    RestClient restClient = RestClient.builder(hosts.toArray(new HttpHost[0]))
        .setRequestConfigCallback(
            new RestClientBuilder.RequestConfigCallback() {
              @Override
              public RequestConfig.Builder customizeRequestConfig(
                      RequestConfig.Builder requestConfigBuilder) {
                  return requestConfigBuilder
                      .setConnectTimeout(5000)
                      .setSocketTimeout(60000);
              }
            })
        .setHttpClientConfigCallback(authenticator().generateAuthenticator())
        .setDefaultHeaders(authenticator().generateDefaultHeaders())
        .build();

    // Create the transport with a Jackson mapper
    ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

    // And create the API client
    client = new ElasticsearchClient(transport);
  }

  ElasticAuthenticator authenticator() {
    return getAuthenticator() == null ? new ElasticNoOpAuthenticator() : getAuthenticator();
  }
  @Override
  protected void stopConnection() {
  }

  @Override
  protected void closeConnection() {
    
  }

}
