package com.adaptris.core.elastic.actions;

import javax.validation.constraints.NotNull;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.elastic.DocumentAction;
import com.adaptris.core.elastic.DocumentWrapper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;

/**
 * An explicitly configured document action.
 * 
 * @config elastic-configured-action
 */
@XStreamAlias("elastic-configured-action")
@ComponentProfile(summary = "An explicitly configured document action", since = "3.9.1")
public class ConfiguredAction implements ActionExtractor {

  /** What's the action you want to do in elastic.
   * 
   */
  @NotNull
  @InputFieldDefault(value = "INDEX")
  @InputFieldHint(expression = true, style = "com.adaptris.core.elastic.DocumentAction")
  @Getter
  @Setter
  private String action;

  public ConfiguredAction() {
    setAction(DocumentAction.INDEX.name());
  }

  @Override
  public String extract(AdaptrisMessage msg, DocumentWrapper document) {
    return msg.resolve(getAction());
  }

  public ConfiguredAction withAction(String action) {
    setAction(action);
    return this;
  }
}
