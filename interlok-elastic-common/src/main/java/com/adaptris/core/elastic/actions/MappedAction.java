package com.adaptris.core.elastic.actions;

import javax.validation.constraints.NotNull;
import org.apache.http.util.Args;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.core.elastic.DocumentWrapper;
import com.adaptris.util.KeyValuePairList;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Wraps a ActionExtractor implementation which is then mapped onto a different action.
 * 
 * @config elastic-mapped-action
 * 
 */
@XStreamAlias("elastic-mapped-action")
@ComponentProfile(summary = "Wraps a ActionExtractor implementation which is then mapped onto a different action", since = "3.9.1")
public class MappedAction implements ActionExtractor {

  @NotNull
  @InputFieldDefault(value = "INDEX")
  private ActionExtractor action;
  @NotNull
  private KeyValuePairList mappings;
  
  public MappedAction() {
    setMappings(new KeyValuePairList());
    setAction(new ConfiguredAction());
  }

  @Override
  public String extract(AdaptrisMessage msg, DocumentWrapper document) throws ServiceException {
    String action = getAction().extract(msg, document);
    String mappedAction = mappings.getValue(action);
    return mappedAction != null ? mappedAction : action;
  }

  public ActionExtractor getAction() {
    return action;
  }

  public void setAction(ActionExtractor action) {
    this.action = Args.notNull(action, "action");
  }

  public MappedAction withAction(ActionExtractor action) {
    setAction(action);
    return this;
  }

  public KeyValuePairList getMappings() {
    return mappings;
  }

  public void setMappings(KeyValuePairList mappings) {
    this.mappings = Args.notNull(mappings, "mappings");
  }

  public MappedAction withMappings(KeyValuePairList mappings) {
    setMappings(mappings);
    return this;
  }
}
