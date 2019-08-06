package com.adaptris.core.elastic.actions;

import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.ObjectUtils;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.elastic.DocumentWrapper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Derive a document action from metadata.
 * 
 * @config elastic-metadata-action
 */
@XStreamAlias("elastic-metadata-action")
@ComponentProfile(summary = "Derive a document action from metadata", since = "3.9.1")
public class MetadataAction implements ActionExtractor {

  @NotNull
  @InputFieldDefault(value = "action")
  private String metadataKey;

  @Override
  public String extract(AdaptrisMessage msg, DocumentWrapper document) {
    return msg.getMetadataValue(metadataKey());
  }

  public String getMetadataKey() {
    return metadataKey;
  }

  public void setMetadataKey(String metadataKey) {
    this.metadataKey = metadataKey;
  }
  
  private String metadataKey() {
    return ObjectUtils.defaultIfNull(getMetadataKey(), "action");
  }

  public MetadataAction withKey(String s) {
    setMetadataKey(s);
    return this;
  }
}
