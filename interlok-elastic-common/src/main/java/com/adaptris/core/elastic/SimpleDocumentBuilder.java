/*
 * Copyright Adaptris Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.adaptris.core.elastic;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.xcontent.XContentBuilder;

/**
 * Builds a simple document for elastic search.
 *
 * <p>
 * The simple document that is created contains the following characteristics
 * <ul>
 * <li>{@code content} contains the message payload (as a String)</li>
 * <li>{@code metadata} all the metadata (removing illegal values, such as metadata keys with '.' in them)</li>
 * <li>{@code date} contains the current date/time</li>
 * <li>The message's uniqueID is used as the ID of the document.
 * </ul>
 * </p>
 *
 * @config elastic-simple-document-builder
 *
 */
@XStreamAlias("elastic-simple-document-builder")
@ComponentProfile(summary = "Build a document for elasticsearch using the raw payload and metadata", since = "3.9.1")
@NoArgsConstructor
@Slf4j
public class SimpleDocumentBuilder implements ElasticDocumentBuilder {

  @Override
  public Iterable<DocumentWrapper> build(AdaptrisMessage msg) throws ProduceException {
    List<DocumentWrapper> result = new ArrayList<>();
    try {
      XContentBuilder builder = jsonBuilder();
      builder.startObject();
      builder.field("content", new Text(msg.getContent()));
      builder.field("metadata", filterIllegal(msg.getMessageHeaders()));
      builder.field("date", new Date());
      builder.endObject();
      result.add(new DocumentWrapper(msg.getUniqueId(), builder));
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return result;
  }

  private static Map<String, String> filterIllegal(Map<String, String> map) {
    Map<String, String> result = new HashMap<>();
    map.entrySet().stream().filter(e -> !e.getKey().contains(".")).forEach(e -> result.put(e.getKey(), e.getValue()));
    return result;
  }
}
