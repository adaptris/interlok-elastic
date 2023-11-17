package com.adaptris.core.elastic.rest;

import com.adaptris.core.http.apache.request.RequestInterceptorBuilder;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.validation.constraints.ConfigDeprecated;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import interlok.http.apache.async.AsyncWithInterceptors;
import interlok.http.apache.async.HttpAsyncClientBuilderConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.validation.Valid;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClientBuilder;

/**
 * Allows you to explicitly control the underlying high level rest client.
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
  @Deprecated(since = "4.5.0")
  @ConfigDeprecated(removalVersion = "5.1.0", message = "Use 'async-http-client-config' instead", groups = Deprecated.class)
  private List<RequestInterceptorBuilder> requestInterceptors;

  /**
   * Additional configuration required for the underlying REST client.
   * <p>
   * This is used to customise the underlying apache {@code CloseableHttpClient} instance used by a {@code RestClient}
   * instance.
   * </p>
   */
  @Valid
  @Getter
  @Setter
  private HttpAsyncClientBuilderConfig asyncHttpClientConfig;

  private transient boolean requestInterceptorWarningLogged = false;

  @Override
  public RestClientBuilder configure(RestClientBuilder builder) {
    builder.setHttpClientConfigCallback(this::configure);
    return builder;
  }

  private HttpAsyncClientBuilder configure(HttpAsyncClientBuilder httpBuilder) {
    try {
      builderConfig().configure(httpBuilder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return httpBuilder;
  }

  @Deprecated(since="4.5.0")
  public AdvancedElasticRestClientCreator withRequestInterceptors(List<RequestInterceptorBuilder> list) {
    setRequestInterceptors(list);
    return this;
  }

  @Deprecated(since="4.5.0")
  public AdvancedElasticRestClientCreator withRequestInterceptors(RequestInterceptorBuilder... list) {
    return withRequestInterceptors(new ArrayList<>(List.of(list)));
  }

  public AdvancedElasticRestClientCreator withAsyncClientBuilderConfig(HttpAsyncClientBuilderConfig cfg) {
    setAsyncHttpClientConfig(cfg);
    return this;
  }

  private HttpAsyncClientBuilderConfig convertToBuilderConfig(List<RequestInterceptorBuilder> list) {
    LoggingHelper.logWarning(requestInterceptorWarningLogged, () -> requestInterceptorWarningLogged = true,
        "Use of request-interceptors is deprecated; switch to a async-http-client-builder instead");
    return (HttpAsyncClientBuilderConfig) new AsyncWithInterceptors().withInterceptors(list);
  }

  private HttpAsyncClientBuilderConfig builderConfig() {
    return Optional.ofNullable(getRequestInterceptors())
        .map(this::convertToBuilderConfig)
        .orElse(HttpAsyncClientBuilderConfig.defaultIfNull(this.getAsyncHttpClientConfig()));
  }
}
