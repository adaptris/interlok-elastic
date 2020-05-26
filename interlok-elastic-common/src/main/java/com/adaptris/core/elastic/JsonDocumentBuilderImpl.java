/*
    Copyright Adaptris Ltd.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.adaptris.core.elastic;

import static org.apache.commons.lang3.StringUtils.isBlank;
import java.io.IOException;
import java.util.Date;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adaptris.annotation.AdvancedConfig;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
public abstract class JsonDocumentBuilderImpl implements ElasticDocumentBuilder {

  protected transient Logger log = LoggerFactory.getLogger(this.getClass());

  /**
   * The field which you want to add the ms since epoch to.
   * <p>
   * If not explicitly specified, the timestamp will not be emitted
   * </p>
   */
  @AdvancedConfig
  @Getter
  @Setter
  private String addTimestampField;


  @SuppressWarnings("unchecked")
  public <T extends JsonDocumentBuilderImpl> T withAddTimestampField(String s) {
    setAddTimestampField(s);
    return (T) this;
  }

  protected ObjectNode addTimestamp(ObjectNode b) {
    if (!isBlank(getAddTimestampField())) {
      b.put(getAddTimestampField(), new Date().getTime());
    }
    return b;
  }

  protected XContentBuilder jsonBuilder(ObjectNode node) throws IOException {
    // Add the TS first.
    ObjectNode withTs = addTimestamp(node);
    return jsonBuilder(withTs.toString());
  }

  protected XContentBuilder jsonBuilder(String jsonString) throws IOException {
    XContentBuilder jsonContent = XContentFactory.jsonBuilder();
    // According to the ES docs; this is the rare situation where using NamedXContentRegistry.EMPTY is fine
    // as we aren't ever calling XContentParser#namedObject(Class, String, Object)...
    try (XContentParser p = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY,
        DeprecationHandler.THROW_UNSUPPORTED_OPERATION, jsonString)) {
      jsonContent.copyCurrentStructure(p);
    }
    return jsonContent;
  }
}
