package com.adaptris.core.elastic.actions;

import javax.validation.constraints.NotNull;

import org.apache.http.util.Args;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.elastic.DocumentAction;
import com.adaptris.core.elastic.DocumentWrapper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * An explicitly configured document action.
 * 
 * @config elastic-configured-action
 */
@XStreamAlias("elastic-configured-action")
@ComponentProfile(summary = "An explicitly configured document action", since = "3.9.1")
public class ConfiguredAction implements ActionExtractor {

  @NotNull
  @InputFieldDefault(value = "INDEX")
  @InputFieldHint(expression = true, style = "com.adaptris.core.elastic.DocumentAction")
  private String action;

  public ConfiguredAction() {
    setAction(DocumentAction.INDEX.name());
  }

  @Override
  public String extract(AdaptrisMessage msg, DocumentWrapper document) {
    return msg.resolve(getAction());
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = Args.notNull(action, "action");
  }

  public ConfiguredAction withAction(String action) {
    setAction(action);
    return this;
  }
}
