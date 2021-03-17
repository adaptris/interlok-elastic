package com.adaptris.core.elastic;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.elastic.csv.BasicFormatBuilder;
import com.adaptris.core.elastic.csv.BasicFormatBuilder.Style;
import com.adaptris.core.elastic.fields.NoOpFieldNameMapper;
import com.adaptris.interlok.util.CloseableIterable;

public class CsvDocumentBuilderTest extends CsvBuilderCase {

  @Override
  protected CSVDocumentBuilder createBuilder() {
    return new CSVDocumentBuilder().withUseHeaderRecord(true).withUniqueIdField(0).withAddTimestampField(null)
        .withFieldNameMapper(new NoOpFieldNameMapper()).withFormat(new BasicFormatBuilder(Style.DEFAULT));
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
