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
import static org.apache.commons.lang.StringUtils.isBlank;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.services.splitter.json.JsonProvider.JsonStyle;
import com.adaptris.core.util.CloseableIterable;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LoggingHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JsonSmartJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.thoughtworks.xstream.annotations.XStreamAlias;

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
public class JsonArrayDocumentBuilder extends JsonDocumentBuilderImpl {

  public static final String UID_PATH = "$.uniqueid";

  @AdvancedConfig(rare = true)
  @Deprecated
  @Removal(version = "3.10.0")
  private Integer bufferSize;
  @AdvancedConfig
  @InputFieldDefault(value = UID_PATH)
  private String uniqueIdJsonPath;
  @AdvancedConfig
  private String routingJsonPath;
  @InputFieldDefault(value = "JSON_ARRAY")
  private JsonStyle jsonStyle;

  private transient boolean bufferSizeWarningLogged = false;

  public JsonArrayDocumentBuilder() {

  }

  @Override
  public Iterable<DocumentWrapper> build(AdaptrisMessage msg) throws ProduceException {
    try {
      if (getBufferSize() != null) {
        LoggingHelper.logWarning(bufferSizeWarningLogged, () -> {
          bufferSizeWarningLogged = true;
        }, "BufferSize is deprecated, and will be ignored");
      }
      return new JsonDocumentWrapper(jsonStyle(), msg);
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

  public <T extends JsonArrayDocumentBuilder> T withJsonStyle(JsonStyle p) {
    setJsonStyle(p);
    return (T) this;
  }

  /**
   * Specify how the payload is parsed to provide JSON objects.
   * 
   * @param p the provider; default is JSON_ARRAY.
   */
  public void setJsonStyle(JsonStyle p) {
    jsonStyle = p;
  }

  public JsonStyle getJsonStyle() {
    return jsonStyle;
  }

  protected JsonStyle jsonStyle() {
    return ObjectUtils.defaultIfNull(getJsonStyle(), JsonStyle.JSON_ARRAY);
  }

  /**
   * 
   * @deprecated since 3.9.3 to support JSON lines, we no longer parse the JSON directly, this setting is ignored
   * 
   */
  @Deprecated
  @Removal(version = "3.10.0", message = "To support JSON lines, we no longer parse the JSON directly, this setting is ignored")
  public Integer getBufferSize() {
    return bufferSize;
  }

  /**
   * Set the internal buffer size.
   * <p>
   * This is used when; the default buffer size matches the default buffer size in {@link BufferedReader} and {@link BufferedWriter}
   * , changes to the buffersize will impact performance and memory usage depending on the underlying operating system/disk.
   * </p>
   * 
   * @param b the buffer size (default is 8192).
   * @deprecated since 3.9.3 to support JSON lines, we no longer parse the JSON directly.
   */
  @Deprecated
  @Removal(version = "3.10.0", message = "To support JSON lines, we no longer parse the JSON directly, this setting is ignored")
  public void setBufferSize(Integer b) {
    this.bufferSize = b;
  }

  /**
   * 
   * @deprecated since 3.9.3 to support JSON lines, we no longer parse the JSON directly.
   * @return
   */
  @Deprecated
  @Removal(version = "3.10.0")
  public JsonArrayDocumentBuilder withBufferSize(Integer i) {
    setBufferSize(i);
    return this;
  }

  public String getUniqueIdJsonPath() {
    return uniqueIdJsonPath;
  }

  /**
   * Specify the json path to the unique id.
   * 
   * @param s the json path to the unique-id; if not specified {@code $.uniqueid}
   */
  public void setUniqueIdJsonPath(String s) {
    this.uniqueIdJsonPath = s;
  }

  public JsonArrayDocumentBuilder withUniqueIdJsonPath(String s) {
    setUniqueIdJsonPath(s);
    return this;
  }

  public String getRoutingJsonPath() {
    return routingJsonPath;
  }

  /**
   * Set the JSON path to extract the routing information.
   * 
   * @param path the path to routing information, defaults to null if not specified.
   */
  public void setRoutingJsonPath(String path) {
    this.routingJsonPath = path;
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


    public JsonDocumentWrapper(JsonStyle style, AdaptrisMessage msg) throws Exception {
      mapper = new ObjectMapper();
      jsonIterable = style.createIterator(msg);
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
            result = new DocumentWrapper(get(ctx, uidPath()), jsonContent).withRouting(getQuietly(ctx, getRoutingJsonPath()));
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
    @SuppressWarnings("deprecation")
    public void close() throws IOException {
      IOUtils.closeQuietly(jsonIterable);
    }

  }

}
