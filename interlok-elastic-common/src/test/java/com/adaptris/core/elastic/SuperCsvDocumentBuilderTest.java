package com.adaptris.core.elastic;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.elastic.csv.BasicFormatBuilder;
import com.adaptris.core.elastic.fields.NoOpFieldNameMapper;
import com.adaptris.csv.BasicPreferenceBuilder;
import com.adaptris.csv.BasicPreferenceBuilder.Style;
import com.adaptris.interlok.util.CloseableIterable;
import com.jayway.jsonpath.ReadContext;
import org.elasticsearch.common.Strings;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SuperCsvDocumentBuilderTest extends CsvBuilderCase {

  @Override
  protected CSVDocumentBuilder createBuilder() {
    return new CSVDocumentBuilder().withUseHeaderRecord(true).withUniqueIdField(0).withAddTimestampField(null)
        .withFieldNameMapper(new NoOpFieldNameMapper()).withPreferences(new BasicPreferenceBuilder(Style.STANDARD_PREFERENCE));
  }

  @Test
  public void testBuild_NoHeaders() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    msg.addMetadata(testName.getMethodName(), testName.getMethodName());
    CSVDocumentBuilder documentBuilder = createBuilder().withUseHeaderRecord(false);
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
      }
    }
    // No headers so the count is the data count + 1
    assertEquals(6, count);
  }

  @Test
  public void testInvalidArgumentsNone() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    msg.addMetadata(testName.getMethodName(), testName.getMethodName());
    CSVDocumentBuilder documentBuilder = createBuilder().withUseHeaderRecord(false).withPreferences(null).withFormat(null);

    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      fail();
    } catch (CoreException e) {
      // expected
    }
  }

  @Test
  public void testInvalidArgumentsTooMany() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    msg.addMetadata(testName.getMethodName(), testName.getMethodName());
    CSVDocumentBuilder documentBuilder = createBuilder().withUseHeaderRecord(false).withFormat(new BasicFormatBuilder());

    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
      }
    }
    // No headers so the count is the data count + 1
    assertEquals(6, count);
  }

  @Test
  public void testFormatAndPreference() throws Exception {
    CSVDocumentBuilder preference = createBuilder();
    CSVDocumentBuilder format = new CsvDocumentBuilderTest().createBuilder();

    AdaptrisMessage pMessage = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    AdaptrisMessage fMessage = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);

    Map<String, String> pIDs = new HashMap<>();
    Map<String, String> fIDs = new HashMap<>();

    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(preference.build(pMessage))) {
      for (DocumentWrapper doc : docs) {
        ReadContext context = parse(Strings.toString(doc.content()));
        pIDs.put(context.read(JSON_PRODUCTUNIQUEID), doc.uniqueId());
      }
    }
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(format.build(fMessage))) {
      for (DocumentWrapper doc : docs) {
        ReadContext context = parse(Strings.toString(doc.content()));
        fIDs.put(context.read(JSON_PRODUCTUNIQUEID), doc.uniqueId());
      }
    }

    assertTrue(pIDs.size() == fIDs.size());
    for (String key : pIDs.keySet()) {
      assertTrue(fIDs.containsKey(key));
      assertEquals(pIDs.get(key), fIDs.get(key));
    }
  }

}
