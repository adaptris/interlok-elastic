package com.adaptris.core.elastic.rest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.CoreException;
import com.adaptris.core.http.apache.request.RequestInterceptorBuilder;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

@XStreamAlias("advanced-elastic-rest-client")
@NoArgsConstructor
public class AdvancedElasticRestClientCreator extends ElasticRestClientCreator {

  /**
   * A list of request interceptors that will be used to configure the rest client.
   * <p>
   * This is the list of interceptors used to customize the underlying apache {@code CloseableHttpClient}
   * instance used by a {@code RestClient} instance.
   * </p>
   */
  @Valid
  @XStreamImplicit
  @Getter
  @Setter
  @NotNull
  @NonNull
  private List<RequestInterceptorBuilder> requestInterceptors = new ArrayList<>();

  @Override
  public RestClientBuilder configure(RestClientBuilder builder) {
    builder.setHttpClientConfigCallback((callback) -> configure(callback));
    return builder;
  }

  private HttpAsyncClientBuilder configure(HttpAsyncClientBuilder httpBuilder) {
    getRequestInterceptors().stream()
        .forEachOrdered((builder) -> httpBuilder.addInterceptorLast(builder.build()));
    return httpBuilder;
  }

  public AdvancedElasticRestClientCreator withRequestInterceptors(
      List<RequestInterceptorBuilder> list) {
    setRequestInterceptors(list);
    return this;
  }

  public AdvancedElasticRestClientCreator withRequestInterceptors(
      RequestInterceptorBuilder... list) {
    return withRequestInterceptors(new ArrayList<>(Arrays.asList(list)));
  }

}
