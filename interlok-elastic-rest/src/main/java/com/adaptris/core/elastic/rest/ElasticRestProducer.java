package com.adaptris.core.elastic.rest;

import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotBlank;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.RequestReplyProducerImp;
import com.adaptris.core.util.DestinationHelper;
import com.adaptris.interlok.util.Args;
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
   * The elastic index to target
   *
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  @NotBlank
  private String index;

  @Override
  public void prepare() throws CoreException {
    Args.notBlank(getIndex(), "index");
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
    return DestinationHelper.resolveProduceDestination(getIndex(), msg);
  }

}
