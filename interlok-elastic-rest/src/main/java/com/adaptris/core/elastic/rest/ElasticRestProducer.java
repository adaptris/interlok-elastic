package com.adaptris.core.elastic.rest;

import static com.adaptris.core.util.DestinationHelper.logWarningIfNotNull;
import static com.adaptris.core.util.DestinationHelper.mustHaveEither;
import java.util.concurrent.TimeUnit;
import javax.validation.Valid;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.RequestReplyProducerImp;
import com.adaptris.core.util.DestinationHelper;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.util.TimeInterval;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Base class for ElasticSearch based activities.
 *
 */
@NoArgsConstructor
public abstract class ElasticRestProducer extends RequestReplyProducerImp {

  private static final TimeInterval TIMEOUT = new TimeInterval(2L, TimeUnit.MINUTES);

  /**
   * The destination is the index.
   *
   */
  @Getter
  @Setter
  @Deprecated
  @Valid
  @Removal(version = "4.0.0", message = "Use 'index' instead")
  private ProduceDestination destination;

  /**
   * The elastic index to target
   *
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  // Needs to be @NotBlank when destination is removed.
  private String index;

  private transient boolean destWarning;


  @Override
  public void prepare() throws CoreException {
    logWarningIfNotNull(destWarning, () -> destWarning = true, getDestination(),
        "{} uses destination, use 'index' instead", LoggingHelper.friendlyName(this));
    mustHaveEither(getIndex(), getDestination());
  }

  @Override
  protected long defaultTimeout() {
    return TIMEOUT.toMilliseconds();
  }

  @SuppressWarnings("unchecked")
  public <T extends ElasticRestProducer> T withIndex(String index) {
    setIndex(index);
    return (T) this;
  }

  @Override
  protected void doProduce(AdaptrisMessage msg, String endpoint) throws ProduceException {
    doRequest(msg, endpoint, defaultTimeout());
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return DestinationHelper.resolveProduceDestination(getIndex(), getDestination(), msg);
  }

}
