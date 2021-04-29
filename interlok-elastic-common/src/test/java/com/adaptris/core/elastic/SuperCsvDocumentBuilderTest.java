package com.adaptris.core.elastic;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.elastic.fields.NoOpFieldNameMapper;
import com.adaptris.csv.BasicPreferenceBuilder;
import com.adaptris.csv.BasicPreferenceBuilder.Style;
import com.adaptris.interlok.util.CloseableIterable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
}
