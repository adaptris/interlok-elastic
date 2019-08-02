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

import static org.apache.commons.lang.StringUtils.isBlank;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.CloseableIterable;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.util.NumberUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JsonSmartJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Parse a json array and create documents from it for elasticsearch
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
@ComponentProfile(summary = "Build documents for elasticsearch from a existing JSON array", since = "3.9.1")
public class JsonArrayDocumentBuilder extends JsonDocumentBuilderImpl {

  private static final int DEFAULT_BUFFER_SIZE = 8192;
  public static final String UID_PATH = "$.uniqueid";

  private transient Logger log = LoggerFactory.getLogger(this.getClass());

  @AdvancedConfig
  @InputFieldDefault(value = "8192")
  private Integer bufferSize;
  @AdvancedConfig
  @InputFieldDefault(value = UID_PATH)
  private String uniqueIdJsonPath;
  @AdvancedConfig
  private String routingJsonPath;

  public JsonArrayDocumentBuilder() {

  }

  @Override
  public Iterable<DocumentWrapper> build(AdaptrisMessage msg) throws ProduceException {
    try {
      ObjectMapper mapper = new ObjectMapper();
      // The buffered read will be closed during the iterator close.
      BufferedReader buf = new BufferedReader(msg.getReader(), bufferSize()); // lgtm
      JsonParser parser = mapper.getFactory().createParser(buf).configure(Feature.AUTO_CLOSE_SOURCE, true);
      if (parser.nextToken() != JsonToken.START_ARRAY) {
        throw new ProduceException("Expected an array");
      }
      return new JsonDocumentWrapper(mapper, parser);
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
   */
  public void setBufferSize(Integer b) {
    this.bufferSize = b;
  }

  int bufferSize() {
    return NumberUtils.toIntDefaultIfNull(getBufferSize(), DEFAULT_BUFFER_SIZE);
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
    private final JsonParser parser;
    private final ObjectMapper mapper;

    private DocumentWrapper nextMessage;
    private transient Configuration jsonConfig = new Configuration.ConfigurationBuilder().jsonProvider(new JsonSmartJsonProvider())
        .mappingProvider(new JacksonMappingProvider()).options(EnumSet.noneOf(Option.class)).build();
    private boolean iteratorInvoked = false;

    public JsonDocumentWrapper(ObjectMapper mapper, JsonParser parser) {
      this.mapper = mapper;
      this.parser = parser;
    }

    @Override
    public DocumentWrapper next() {
      DocumentWrapper result = nextMessage;
      nextMessage = null;
      return result;
    }

    private DocumentWrapper buildNext() throws IOException {
      DocumentWrapper result = null;
      if (parser.nextToken() == JsonToken.START_OBJECT) {
        ObjectNode node = addTimestamp(mapper.readTree(parser));
        // Add the timestamp before we start futzing with jsonBuilder...
        addTimestamp(node);
        String jsonString = node.toString();
        XContentBuilder jsonContent = jsonBuilder(jsonString);
        ReadContext ctx = JsonPath.parse(jsonString, jsonConfig);
        result = new DocumentWrapper(get(ctx, uidPath()), jsonContent)
            .withRouting(getQuietly(ctx, getRoutingJsonPath()));
      }
      return result;
    }

    private String get(ReadContext ctx, String path) {
      return ctx.read(path);
    }

    private String getQuietly(ReadContext ctx, String path) {
      String result = null;
      if (isBlank(path)) {
        return null;
      }
      try {
        result = get(ctx, path);
      } catch (PathNotFoundException e) {
        result = null;
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
        try {
          nextMessage = buildNext();
        }
        catch (IOException e) {
          log.warn("Could not construct next DocumentWrapper; badly formed JSON?", e);
          throw new RuntimeException("Could not construct next DocumentWrapper", e);
        }
      }
      return nextMessage != null;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void close() throws IOException {
      IOUtils.closeQuietly(parser);
    }
  }


}
