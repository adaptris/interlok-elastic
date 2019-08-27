package com.adaptris.core.elastic.rest;

import java.util.concurrent.TimeUnit;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.RequestReplyProducerImp;
import com.adaptris.util.TimeInterval;

/**
 * Base class for ElasticSearch based activities.
 * 
 */
public abstract class ElasticRestProducer extends RequestReplyProducerImp {

  private static final TimeInterval TIMEOUT = new TimeInterval(2L, TimeUnit.MINUTES);

  public ElasticRestProducer() {}

  @Override
  public void prepare() throws CoreException {}

  @Override
  protected long defaultTimeout() {
    return TIMEOUT.toMilliseconds();
  }

  @Override
  public void produce(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    request(msg, destination, defaultTimeout());
  }

  public <T extends ElasticRestProducer> T withDestination(ProduceDestination d) {
    setDestination(d);
    return (T) this;
  }

}
