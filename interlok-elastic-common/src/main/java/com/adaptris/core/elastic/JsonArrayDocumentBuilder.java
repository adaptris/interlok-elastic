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

import static com.adaptris.core.elastic.JsonHelper.get;
import static com.adaptris.core.elastic.JsonHelper.getQuietly;
import static org.apache.commons.lang3.StringUtils.isBlank;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.services.splitter.json.JsonProvider.JsonStyle;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.util.CloseableIterable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JsonSmartJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Parse a json array (or json lines format)and create documents from it for elasticsearch
 *
 * <p>
 * The unique-id for each document created is derived from the {@link JsonArrayDocumentBuilder#getUniqueIdJsonPath()} which defaults
 * to {@code $.uniqueid}
 * </p>
 *
 * @config elastic-json-array-document-builder
 *
 */
@XStreamAlias("elastic-json-array-document-builder")
@ComponentProfile(summary = "Build documents for elasticsearch from a existing JSON array/JSON lines doc", since = "3.9.1")
@DisplayOrder(order = {"jsonStyle", "addTimestampField", "uniqueIdJsonPath", "routingJsonPath"})
@NoArgsConstructor
public class JsonArrayDocumentBuilder extends JsonDocumentBuilderImpl {

  public static final String UID_PATH = "$.uniqueid";

  /**
   * The json path to the unique id.
   *
   * <p>
   * If not specified explicitly then defaults to {@code $.uniqueid}
   * </p>
   */
  @AdvancedConfig
  @InputFieldDefault(value = UID_PATH)
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  private String uniqueIdJsonPath;
  /**
   * Set the JSON path to extract the routing information.
   *
   *
   */
  @AdvancedConfig
  @Getter
  @Setter
  private String routingJsonPath;
  /**
   * Specify how the payload is parsed to provide JSON objects.
   *
   * <p>
   * If not specified defaults to {@code JSON_ARRAY}
   * </p>
   */
  @InputFieldDefault(value = "JSON_ARRAY")
  @Getter
  @Setter
  private JsonStyle jsonStyle;

  @Override
  public Iterable<DocumentWrapper> build(AdaptrisMessage msg) throws ProduceException {
    try {
      return new JsonDocumentWrapper(jsonStyle(), msg, msg.resolve(uidPath()));
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
  }

  @Override
  protected ObjectNode addTimestamp(ObjectNode b) {
    if (!isBlank(getAddTimestampField())) {
      b.put(getAddTimestampField(), new Date().getTime());
    }
    return b;
  }

  @SuppressWarnings("unchecked")
  public <T extends JsonArrayDocumentBuilder> T withJsonStyle(JsonStyle p) {
    setJsonStyle(p);
    return (T) this;
  }

  protected JsonStyle jsonStyle() {
    return ObjectUtils.defaultIfNull(getJsonStyle(), JsonStyle.JSON_ARRAY);
  }

  public JsonArrayDocumentBuilder withUniqueIdJsonPath(String s) {
    setUniqueIdJsonPath(s);
    return this;
  }

  public JsonArrayDocumentBuilder withRoutingJsonPath(String s) {
    setRoutingJsonPath(s);
    return this;
  }


  String uidPath() {
    return StringUtils.defaultIfBlank(getUniqueIdJsonPath(), UID_PATH);
  }

  private class JsonDocumentWrapper implements CloseableIterable<DocumentWrapper>, Iterator<DocumentWrapper> {

    private DocumentWrapper nextMessage;
    private final ObjectMapper mapper;
    private transient Configuration jsonConfig = new Configuration.ConfigurationBuilder().jsonProvider(new JsonSmartJsonProvider())
        .mappingProvider(new JacksonMappingProvider()).options(EnumSet.noneOf(Option.class)).build();
    private boolean iteratorInvoked = false;
    private final CloseableIterable<AdaptrisMessage> jsonIterable;
    private final Iterator<AdaptrisMessage> jsonIterator;
    private final String uniqueIdPath;


    public JsonDocumentWrapper(JsonStyle style, AdaptrisMessage msg, String uniqueIdJsonPath) throws Exception {
      mapper = new ObjectMapper();
      jsonIterable = CloseableIterable.ensureCloseable(style.createIterator(msg));
      uniqueIdPath = uniqueIdJsonPath;
      jsonIterator = jsonIterable.iterator();
    }

    @Override
    public DocumentWrapper next() {
      DocumentWrapper result = nextMessage;
      nextMessage = null;
      return result;
    }

    private DocumentWrapper buildNext() {
      DocumentWrapper result = null;
      try {
        if (jsonIterator.hasNext()) {
          AdaptrisMessage msg = jsonIterator.next();
          try (InputStream in = msg.getInputStream()) {
            ObjectNode node = addTimestamp((ObjectNode) mapper.readTree(in));
            String jsonString = node.toString();
            XContentBuilder jsonContent = jsonBuilder(jsonString);
            ReadContext ctx = JsonPath.parse(jsonString, jsonConfig);
            result = new DocumentWrapper(get(ctx, uniqueIdPath), jsonContent).withRouting(getQuietly(ctx, getRoutingJsonPath()));
          }
        }
      } catch (Exception e) {
        log.warn("Could not construct next DocumentWrapper; badly formed JSON?", e);
        throw JsonHelper.wrapAsRuntimeException(e);
      }
      return result;
    }


    @Override
    public Iterator<DocumentWrapper> iterator() {
      if (iteratorInvoked) {
        throw new IllegalStateException("iterator already invoked");
      }
      iteratorInvoked = true;
      return this;
    }

    @Override
    public boolean hasNext() {
      if (nextMessage == null) {
        nextMessage = buildNext();
      }
      return nextMessage != null;
    }

    @Override
    public void close() throws IOException {
      IOUtils.closeQuietly(jsonIterable);
    }

  }

}
