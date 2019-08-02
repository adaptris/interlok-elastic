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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.common.Strings;
import org.junit.Test;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.CloseableIterable;
import com.jayway.jsonpath.ReadContext;

public class JsonDocumentBuilderTest extends BuilderCase {


  @Test
  public void testBuild() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(sampleJsonContent());
    JsonDocumentBuilder documentBuilder = new JsonDocumentBuilder();
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        assertEquals(msg.getUniqueId(), doc.uniqueId());
        assertNull(doc.routing());
        ReadContext context = parse(Strings.toString(doc.content()));
        assertEquals("red", context.read("$.store.bicycle.color"));
      }
    }
    assertEquals(1, count);
  }

  @Test
  public void testBuild_WithTimestamp() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(sampleJsonContent());
    JsonDocumentBuilder documentBuilder = new JsonDocumentBuilder().withAddTimestampField("timestamp");
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        assertEquals(msg.getUniqueId(), doc.uniqueId());
        ReadContext context = parse(Strings.toString(doc.content()));
        assertNotNull(context.read("$.timestamp"));
        assertEquals("red", context.read("$.store.bicycle.color"));
      }
    }
    assertEquals(1, count);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testBuild_NotJson() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    JsonDocumentBuilder documentBuilder = new JsonDocumentBuilder();
    CloseableIterable<DocumentWrapper> docs = null;
    try {
      docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg));
      fail();
    } catch (ProduceException expected) {

    } finally {
      IOUtils.closeQuietly(docs);
    }
  }

  @Test
  public void testBuild_WithRouting() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(sampleJsonContent());
    msg.addMetadata("routing", "MyRoute");
    JsonDocumentBuilder documentBuilder = new JsonDocumentBuilder().withRouting("%message{routing}");
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        assertEquals(msg.getUniqueId(), doc.uniqueId());
        assertNotNull(doc.routing());
        assertEquals("MyRoute", doc.routing());
        ReadContext context = parse(Strings.toString(doc.content()));
        assertEquals("red", context.read("$.store.bicycle.color"));
      }
    }
    assertEquals(1, count);
  }

  public static String sampleJsonContent() {
    return "{"
    + "\"store\": {"
    +    "\"book\": ["
    +        "{"
    +            "\"category\": \"reference\","
    +            "\"author\": \"Nigel Rees\","
    +            "\"title\": \"Sayings of the Century\","
    +            "\"price\": 8.95"
    +        "},"
    +        "{"
    +            "\"category\": \"fiction\","
    +            "\"author\": \"Evelyn Waugh\","
    +            "\"title\": \"Sword of Honour\","
    +            "\"price\": 12.99"
    +        "},"
    +        "{"
    +            "\"category\": \"fiction\","
    +            "\"author\": \"Herman Melville\","
    +            "\"title\": \"Moby Dick\","
    +            "\"isbn\": \"0-553-21311-3\","
    +            "\"price\": 8.99"
    +        "},"
    +        "{"
    +            "\"category\": \"fiction\","
    +            "\"author\": \"J. R. R. Tolkien\","
    +            "\"title\": \"The Lord of the Rings\","
    +            "\"isbn\": \"0-395-19395-8\","
    +            "\"price\": 22.99"
    +        "}"
    +    "],"
    +    "\"bicycle\": {"
    +        "\"color\": \"red\","
    +        "\"price\": 19.95"
    +    "}"
    + "}"
    + "}";
  }
}
