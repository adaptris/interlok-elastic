package com.adaptris.core.elastic.fields;

import com.adaptris.annotation.ComponentProfile;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @config elastic-uppercase-field-name-mapper
 */
@XStreamAlias("elastic-uppercase-field-name-mapper")
@ComponentProfile(summary = "Map a fieldname to its uppercase variant", since = "3.9.1")
public class ToUpperCaseFieldNameMapper implements FieldNameMapper {

  @Override
  public String map(String name) {
    return name.toUpperCase();
  }

}
