package com.adaptris.core.elastic.rest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import org.apache.commons.io.IOUtils;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.NoOpConnection;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * Connect to elasticsearch via their high level REST client
 * 
 * @config elastic-rest-connection
 */
@XStreamAlias("elastic-rest-connection")
@ComponentProfile(summary = "Connect to elasticsearch via their high level REST client",
    since = "3.9.1")
public class ElasticRestConnection extends NoOpConnection implements TransportClientProvider {

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

  /**
   * Controls how the underlying {@code RestClient} is configured.
   * 
   * <p>
   * By default defaults to {@link ElasticRestClientCreator} if not explicitly confgured. You may
   * need to use {@link AdvancedElasticRestClientCreator} if you are connecting to an instance that
   * has specific requirements (e.g. AWS Managed Elastic).
   * </p>
   */
  @NotNull
  @NonNull
  @Getter
  @Setter
  @Valid
  private ElasticClientCreator elasticClientCreator;
  
  private transient TransportClient transportClient = null;

  public ElasticRestConnection() {
    setElasticClientCreator(new ElasticRestClientCreator());
  }

  public ElasticRestConnection(String... urls) {
    this();
    setTransportUrls(new ArrayList<String>(Arrays.asList(urls)));
  }

  @Override
  @SuppressWarnings("deprecation")
  protected void closeConnection() {
    IOUtils.closeQuietly(transportClient);
    transportClient = null;
  }

  @Override
  public TransportClient getTransport() throws Exception {
    if (transportClient == null) {
      transportClient = getElasticClientCreator().createTransportClient(getTransportUrls());
    }
    return transportClient;
  }

  public ElasticRestConnection withElasticClientCreator(ElasticClientCreator creator) {
    setElasticClientCreator(creator);
    return this;
  } 

}
