package com.adaptris.core.elastic.fields;

import com.adaptris.annotation.ComponentProfile;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

/**
 * @config elastic-lowercase-field-name-mapper
 */
@XStreamAlias("elastic-lowercase-field-name-mapper")
@ComponentProfile(summary = "Map a fieldname to its lowercase variant", since = "3.9.1")
@NoArgsConstructor
public class ToLowerCaseFieldNameMapper implements FieldNameMapper {

  @Override
  public String map(String name) {
    return name.toLowerCase();
  }

}
