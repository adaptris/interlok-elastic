package com.adaptris.core.elastic.actions;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.core.elastic.DocumentAction;
import com.adaptris.core.elastic.DocumentWrapper;

@FunctionalInterface
public interface ActionExtractor {
  /**
   * Extract the action from the document or message.
   *
   * @param msg the associated message (for metadata)
   * @param document the {@link DocumentWrapper}.
   * @return the action that generally maps to {@link DocumentAction}.
   */
  String extract(AdaptrisMessage msg, DocumentWrapper document) throws ServiceException;
}
