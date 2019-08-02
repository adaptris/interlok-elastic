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
import org.elasticsearch.common.Strings;
import org.junit.Test;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ProduceException;
import com.adaptris.core.util.CloseableIterable;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;

public class JsonArrayDocumentBuilderTest extends BuilderCase {

  private static AdaptrisMessage createMessage() throws Exception {
    return AdaptrisMessageFactory.getDefaultInstance().newMessage(sampleJsonContent());
  }

  @Test
  public void testBuild() throws Exception {
    AdaptrisMessage msg = createMessage();
    JsonArrayDocumentBuilder builder = new JsonArrayDocumentBuilder();
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(builder.build(msg))) {
      for (DocumentWrapper d : docs) {
        count++;
        assertNotNull(d.uniqueId());
        assertNull(d.routing());
        assertEquals("000" + count, d.uniqueId());
        assertNotNull(d.content());
      }
    }
    assertEquals(4, count);
  }

  @Test
  public void testBuild_WithTimestamp() throws Exception {
    AdaptrisMessage msg = createMessage();
    JsonArrayDocumentBuilder builder = new JsonArrayDocumentBuilder().withAddTimestampField("timestamp");
    builder.setBufferSize(4096);
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(builder.build(msg))) {
      for (DocumentWrapper d : docs) {
        count++;
        assertNotNull(d.uniqueId());
        assertNotNull(d.content());
        assertEquals("000" + count, d.uniqueId());
        ReadContext context = parse(Strings.toString(d.content()));
        assertNotNull(context.read("$.timestamp"));
      }
    }
    assertEquals(4, count);
  }

  @Test
  public void testBuild_WithUniqueIdPath() throws Exception {
    AdaptrisMessage msg = createMessage();
    JsonArrayDocumentBuilder builder = new JsonArrayDocumentBuilder().withUniqueIdJsonPath("$.originorgid");
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(builder.build(msg))) {
      for (DocumentWrapper d : docs) {
        count++;
        assertNotNull(d.uniqueId());
        assertEquals("000" + count, d.uniqueId());
      }
    }
    assertEquals(4, count);
  }

  @Test
  public void testBuild_WithUniqueIdPath_NotFound() throws Exception {
    AdaptrisMessage msg = createMessage();
    JsonArrayDocumentBuilder builder = new JsonArrayDocumentBuilder().withUniqueIdJsonPath("$.umwhat");
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(builder.build(msg))) {
      for (DocumentWrapper d : docs) {
        // iterating over the first doc should cause a RuntimeException
        fail();
      }
    }
    catch (PathNotFoundException expected) {

    }
  }

  @Test
  public void testBuild_WithRoutingId() throws Exception {
    AdaptrisMessage msg = createMessage();
    JsonArrayDocumentBuilder builder = new JsonArrayDocumentBuilder().withRoutingJsonPath("$.originorgid");
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(builder.build(msg))) {
      for (DocumentWrapper d : docs) {
        count++;
        assertNotNull(d.uniqueId());
        assertNotNull(d.routing());
        assertEquals("000" + count, d.uniqueId());
        assertEquals("000" + count, d.routing());
        assertNotNull(d.content());
      }
    }
    assertEquals(4, count);
  }

  @Test
  public void testBuild_WithParentId() throws Exception {
    AdaptrisMessage msg = createMessage();
    JsonArrayDocumentBuilder builder = new JsonArrayDocumentBuilder();
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(builder.build(msg))) {
      for (DocumentWrapper d : docs) {
        count++;
        assertNotNull(d.uniqueId());
        assertNull(d.routing());
        assertEquals("000" + count, d.uniqueId());
        assertNotNull(d.content());
      }
    }
    assertEquals(4, count);
  }

  @Test
  public void testBuild_NotJsonArray() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("{\"Hello\":\"World\"}");
    JsonArrayDocumentBuilder documentBuilder = new JsonArrayDocumentBuilder();
    try ( CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      fail();
    } catch (ProduceException expected) {
    }
  }

  public static String sampleJsonContent() {
    return "[\n"
        + "{\"uniqueid\":\"0001\",\"originorgid\":\"0001\",\"sourceorgid\":\"9999-0001\",\"organisationid\":\"9999\",\"growername\":\"Lewin Chan\",\"fieldid\":\"BED1-8E7025F0105C\",\"fieldname\":\"My Field\",\"fieldcountrycode\":\"GB\",\"fieldadminname1\":\"England\",\"fieldadminname2\":\"Somewhere\",\"fieldadminname3\":\"Somewhereville\",\"fieldadminname4\":\"Somewhere Else\",\"fieldlocation\":{\"latitude\":\"1.1\",\"longitude\":\"1.1\"},\"cropzoneseedvariety\":\"ELS\",\"cropzoneid\":\"ABB7-92797F481763\",\"cropzonecrop\":\"Arable Stewardship\",\"cropzoneofficialarea\":\"0.04\",\"cropzoneworkingarea\":\"0.04\",\"cropyear\":\"2013\",\"fieldsoiltextureclassabbreviation\":\"CL\",\"fieldsoiltextureclassname\":\"Clay Loam\",\"operationtypes\":[],\"appliedprotectionproducts\":[],\"appliednutritionproducts\":[],\"appliedseedvarieties\":[],\"appliedproductcategories\":[],\"appliedactiveingredients\":[],\"appliednutrients\":[],\"displayablevarieties\":[\"ELS\"],\"sortablevarieties\":\"ELS\",\"action\":\"INSERT\",\"creationdatetime\":\"2017-05-04T15:32:34Z\"},\n"
        + "{\"uniqueid\":\"0002\",\"originorgid\":\"0002\",\"sourceorgid\":\"9999-0002\",\"organisationid\":\"9999\",\"growername\":\"Gerco Dries\",\"fieldid\":\"96AF-910E1226A78A\",\"fieldname\":\"Boston Harbor\",\"fieldcountrycode\":\"USA\",\"fieldadminname1\":\"USA\",\"fieldadminname2\":\"Well\",\"fieldadminname3\":\"Springfield\",\"fieldadminname4\":\"\",\"fieldlocation\":{\"latitude\":\"1\",\"longitude\":\"-1\"},\"cropzoneseedvariety\":\"EE3 6m\",\"cropzoneid\":\"A4F6-C15703D7B8A3\",\"cropzonecrop\":\"Arable Stewardship\",\"cropzoneofficialarea\":\"0.58\",\"cropzoneworkingarea\":\"0.58\",\"cropyear\":\"2011\",\"fieldsoiltextureclassabbreviation\":\"CL\",\"fieldsoiltextureclassname\":\"Clay Loam\",\"operationtypes\":[],\"appliedprotectionproducts\":[],\"appliednutritionproducts\":[],\"appliedseedvarieties\":[],\"appliedproductcategories\":[],\"appliedactiveingredients\":[],\"appliednutrients\":[],\"displayablevarieties\":[\"EE3 6m\"],\"sortablevarieties\":\"EE3 6m\",\"action\":\"INSERT\",\"creationdatetime\":\"2017-05-04T15:32:34Z\"},\n"
        + "{\"uniqueid\":\"0003\",\"originorgid\":\"0003\",\"sourceorgid\":\"9999-0003\",\"organisationid\":\"9999\",\"growername\":\"Frankies\",\"fieldid\":\"93CA-58C51B3139BF\",\"fieldname\":\"Bennies\",\"fieldcountrycode\":\"GB\",\"fieldadminname1\":\"England\",\"fieldadminname2\":\"London\",\"fieldadminname3\":\"Mulesoft\",\"fieldadminname4\":\"Tibco\",\"fieldlocation\":{\"latitude\":\"1\",\"longitude\":\"-2\"},\"cropzoneseedvariety\":\"Turnips\",\"cropzoneid\":\"84F3-A6B146177BB9\",\"cropzonecrop\":\"Fodder Crop\",\"cropzoneofficialarea\":\"17.8955285244867\",\"cropzoneworkingarea\":\"17.8955285244867\",\"cropyear\":\"2010\",\"fieldsoiltextureclassabbreviation\":\"CL\",\"fieldsoiltextureclassname\":\"Clay Loam\",\"operationtypes\":[\"Pesticide Application\"],\"appliedprotectionproducts\":[\"Shogun (14567)\"],\"appliednutritionproducts\":[],\"appliedseedvarieties\":[],\"appliedproductcategories\":[\"Herbicides\"],\"appliedactiveingredients\":[\"Propaquizafop\"],\"appliednutrients\":[],\"displayablevarieties\":[\"Turnips\"],\"sortablevarieties\":\"Turnips\",\"action\":\"INSERT\",\"creationdatetime\":\"2017-05-04T15:32:34Z\"},\n"
        + "{\"uniqueid\":\"0004\",\"originorgid\":\"0004\",\"sourceorgid\":\"9999-0004\",\"organisationid\":\"9999\",\"growername\":\"Homer\",\"fieldid\":\"8B50-1AE6270CC42C\",\"fieldname\":\"Block 14\",\"fieldcountrycode\":\"GB\",\"fieldadminname1\":\"England\",\"fieldadminname2\":\"Admiralty Arch\",\"fieldadminname3\":\"The Mall\",\"fieldadminname4\":\"\",\"fieldlocation\":{\"latitude\":\"1\",\"longitude\":\"-0.1\"},\"cropzoneseedvariety\":\"Grass\",\"cropzoneid\":\"8A95-03F58F83BE84\",\"cropzonecrop\":\"Grass\",\"cropzoneofficialarea\":\"3.61\",\"cropzoneworkingarea\":\"3.61\",\"cropyear\":\"2011\",\"fieldsoiltextureclassabbreviation\":\"CL\",\"fieldsoiltextureclassname\":\"Clay Loam\",\"operationtypes\":[],\"appliedprotectionproducts\":[],\"appliednutritionproducts\":[],\"appliedseedvarieties\":[],\"appliedproductcategories\":[],\"appliedactiveingredients\":[],\"appliednutrients\":[],\"displayablevarieties\":[\"Grass\"],\"sortablevarieties\":\"Grass\",\"action\":\"INSERT\",\"creationdatetime\":\"2017-05-04T15:32:34Z\"}\n"
        + "]";
  }
}
