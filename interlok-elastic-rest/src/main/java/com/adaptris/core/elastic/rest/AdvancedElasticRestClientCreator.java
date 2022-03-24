package com.adaptris.core.elastic.rest;

import com.adaptris.core.http.apache.request.RequestInterceptorBuilder;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import interlok.http.apache.credentials.CredentialsProviderBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.validation.Valid;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClientBuilder;

/** Allows you to explicitly control the underlying high level rest client.
 *
 */
@XStreamAlias("advanced-elastic-rest-client")
@NoArgsConstructor
public class AdvancedElasticRestClientCreator extends ElasticRestClientCreator {

  /**
   * A list of request interceptors that will be used to configure the rest client.
   * <p>
   * This is the list of interceptors used to customize the underlying apache {@code CloseableHttpClient} instance used
   * by a {@code RestClient} instance.
   * </p>
   */
  @Valid
  @XStreamImplicit
  @Getter
  @Setter
  private List<RequestInterceptorBuilder> requestInterceptors;

  /**
   * Customise the underlying Apache {@code HttpClientBuilder} to supply credentials.
   */
  // Since clientCallback uses HttpAsyncClientBuilder which doesn't share a hierarchy with HttpClientBuilder
  // which sadly means that we can't use HttpClientConfigurator for more fun times.
  @Valid
  @Getter
  @Setter
  private CredentialsProviderBuilder credentialsProvider;

  @Override
  public RestClientBuilder configure(RestClientBuilder builder) {
    builder.setHttpClientConfigCallback(this::configure);
    return builder;
  }

  private HttpAsyncClientBuilder configure(HttpAsyncClientBuilder httpBuilder) {
    requestInterceptors().stream()
        .forEachOrdered((builder) -> httpBuilder.addInterceptorLast(builder.build()));
    Optional.ofNullable(getCredentialsProvider())
        .ifPresent((cp) -> httpBuilder.setDefaultCredentialsProvider(cp.build()));
    return httpBuilder;
  }

  private List<RequestInterceptorBuilder> requestInterceptors() {
    return ObjectUtils.defaultIfNull(getRequestInterceptors(), Collections.emptyList());
  }

  public AdvancedElasticRestClientCreator withRequestInterceptors(List<RequestInterceptorBuilder> list) {
    setRequestInterceptors(list);
    return this;
  }

  public AdvancedElasticRestClientCreator withRequestInterceptors(RequestInterceptorBuilder... list) {
    return withRequestInterceptors(new ArrayList<>(List.of(list)));
  }

  public AdvancedElasticRestClientCreator withCredentialsProvider(CredentialsProviderBuilder builder) {
    setCredentialsProvider(builder);
    return this;
  }
}
