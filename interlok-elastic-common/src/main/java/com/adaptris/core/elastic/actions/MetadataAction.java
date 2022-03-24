package com.adaptris.core.elastic.actions;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.elastic.DocumentWrapper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import javax.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.lang3.ObjectUtils;

/**
 * Derive a document action from metadata.
 *
 * @config elastic-metadata-action
 */
@XStreamAlias("elastic-metadata-action")
@ComponentProfile(summary = "Derive a document action from metadata", since = "3.9.1")
public class MetadataAction implements ActionExtractor {

  /**
   * The metadata key that provides the action.
   * <p>
   * If not specified the default metadata key is {@code action}
   * </p>
   */
  @NotNull
  @InputFieldDefault(value = "action")
  @Getter
  @Setter
  @NonNull
  private String metadataKey;

  @Override
  public String extract(AdaptrisMessage msg, DocumentWrapper document) {
    return msg.getMetadataValue(metadataKey());
  }

  private String metadataKey() {
    return ObjectUtils.defaultIfNull(getMetadataKey(), "action");
  }

  public MetadataAction withKey(String s) {
    setMetadataKey(s);
    return this;
  }
}
