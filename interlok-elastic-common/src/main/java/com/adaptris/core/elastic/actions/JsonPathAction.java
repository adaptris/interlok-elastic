package com.adaptris.core.elastic.actions;

import javax.validation.constraints.NotNull;
import org.elasticsearch.common.Strings;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.core.elastic.DocumentWrapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * A document action derived from a JSON path.
 * 
 * @config elastic-jsonpath-action
 */
@XStreamAlias("elastic-jsonpath-action")
@ComponentProfile(summary = "A document action derived from a JSON path", since = "3.9.1")
public class JsonPathAction implements ActionExtractor {

  /**
   * The JSON Path to the action.
   * <p>
   * If not explicitly specified then defaults to {@code $.action}
   * </p>
   */
  @NonNull
  @NotNull
  @InputFieldDefault(value = "$.action")
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  private String jsonPath;
  
  public JsonPathAction() {
    setJsonPath("$.action");
  }

  @Override
  public String extract(AdaptrisMessage msg, DocumentWrapper document) throws ServiceException {
    String content = Strings.toString(document.content());
    ReadContext context = JsonPath.parse(content);
    return context.read(msg.resolve(getJsonPath()));
  }

}
