package com.adaptris.core.elastic;

/**
 * What to do with this document.
 */
public enum DocumentAction {
  /** Delete the document */
  DELETE,
  /** Update a document */
  UPDATE,
  /** Index a document */
  INDEX,
  /** Update or insert the document */
  UPSERT,
  NONE
}
