package com.adaptris.core.elastic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.LinkedHashMap;

import org.elasticsearch.common.Strings;
import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ProduceException;
import com.adaptris.core.stubs.DefectiveMessageFactory;
import com.adaptris.core.stubs.DefectiveMessageFactory.WhenToBreak;
import com.adaptris.interlok.util.CloseableIterable;
import com.jayway.jsonpath.ReadContext;

public class SimpleJsonDocumentBuilderTest extends BuilderCase {

  @Test
  public void testBuild() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    msg.addMetadata(getName(), getName());
    msg.addMetadata("hello.world", "should.be.filtered");
    SimpleDocumentBuilder documentBuilder = new SimpleDocumentBuilder();
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        assertEquals(msg.getUniqueId(), doc.uniqueId());
        ReadContext context = parse(Strings.toString(doc.content()));
        assertEquals("Hello World", context.read("$.content"));
        LinkedHashMap<?, ?> metadata = context.read("$.metadata");
        assertTrue(metadata.containsKey(getName()));
        assertFalse(metadata.containsKey("hello.world"));
        assertEquals(getName(), metadata.get(getName()));
      }
    }
    assertEquals(1, count);
  }

  @Test
  public void testBuild_ProduceException() throws Exception {
    AdaptrisMessage msg = new DefectiveMessageFactory(EnumSet.of(WhenToBreak.METADATA_GET)).newMessage("Hello World");
    msg.addMetadata(getName(), getName());
    SimpleDocumentBuilder documentBuilder = new SimpleDocumentBuilder();
    assertThrows(ProduceException.class, () -> {
      try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      }
    });
  }
}
