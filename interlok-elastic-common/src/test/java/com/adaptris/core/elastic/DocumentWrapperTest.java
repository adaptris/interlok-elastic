package com.adaptris.core.elastic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.jupiter.api.Test;

public class DocumentWrapperTest {


  @Test
  public void testContent() throws Exception {
    XContentBuilder builder = content();
    DocumentWrapper wrapper = new DocumentWrapper("id", builder);
    assertEquals(builder, wrapper.content());
  }

  @Test
  public void testUniqueId() throws Exception {
    DocumentWrapper wrapper = new DocumentWrapper("id", content());
    assertEquals("id", wrapper.uniqueId());
  }

  @Test
  public void testRouting() throws Exception {
    DocumentWrapper wrapper = new DocumentWrapper("id", content()).withRouting("routing");
    assertEquals("routing", wrapper.routing());
  }

  @Test
  public void testAction() throws Exception {
    DocumentWrapper wrapper = new DocumentWrapper("id", content()).withAction(DocumentAction.INDEX.name());
    assertEquals(DocumentAction.INDEX, wrapper.action());
  }

  private XContentBuilder content() throws Exception {
    XContentBuilder builder = XContentFactory.jsonBuilder();
    builder.startObject();
    builder.field("field", "value");
    builder.endObject();
    return builder;
  }
}
