package com.adaptris.core.elastic.fields;

import com.adaptris.annotation.ComponentProfile;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @config elastic-noop-field-name-mapper
 */
@XStreamAlias("elastic-noop-field-name-mapper")
@ComponentProfile(summary = "Leave the fieldname as is", since = "3.9.1")
public class NoOpFieldNameMapper implements FieldNameMapper {

  @Override
  public String map(String name) {
    return name;
  }

}
