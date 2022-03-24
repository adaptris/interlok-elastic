package com.adaptris.core.elastic.actions;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.core.elastic.DocumentWrapper;
import com.adaptris.util.KeyValuePairList;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import javax.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.lang3.ObjectUtils;

/**
 * Wraps a ActionExtractor implementation which is then mapped onto a different action.
 *
 * @config elastic-mapped-action
 *
 */
@XStreamAlias("elastic-mapped-action")
@ComponentProfile(
    summary = "Wraps a ActionExtractor implementation which is then mapped onto a different action",
    since = "3.9.1")
public class MappedAction implements ActionExtractor {

  /**
   * The underlying action extractor.
   *
   */
  @NotNull
  @InputFieldDefault(value = "INDEX")
  @Getter
  @Setter
  @NonNull
  private ActionExtractor action;
  /**
   * Mapping the action returned by the extractor into another action.
   * <p>
   * If the key from the list matches the action returned by the extractor, then the associated
   * value is used. Otherwise the extracted action is used.
   * </p>
   */
  @NotNull
  @Getter
  @Setter
  @NonNull
  private KeyValuePairList mappings;

  public MappedAction() {
    setMappings(new KeyValuePairList());
    setAction(new ConfiguredAction());
  }

  @Override
  public String extract(AdaptrisMessage msg, DocumentWrapper document) throws ServiceException {
    String action = getAction().extract(msg, document);
    String mappedAction = mappings.getValue(action);
    return ObjectUtils.defaultIfNull(mappedAction, action);
  }

  public MappedAction withAction(ActionExtractor action) {
    setAction(action);
    return this;
  }

  public MappedAction withMappings(KeyValuePairList mappings) {
    setMappings(mappings);
    return this;
  }
}
