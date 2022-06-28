package com.adaptris.core.elastic.fields;

@FunctionalInterface
public interface FieldNameMapper {
  /**
   * Map the fieldname into something else.
   *
   * @param name the name from the document
   * @return the field name.
   */
  String map(String name);
}
