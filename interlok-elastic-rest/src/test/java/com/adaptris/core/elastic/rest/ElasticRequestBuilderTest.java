package com.adaptris.core.elastic.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.junit.jupiter.api.Test;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.DefaultMessageFactory;
import com.adaptris.core.elastic.DocumentWrapper;
import com.adaptris.core.elastic.SimpleDocumentBuilder;
import com.adaptris.interlok.util.CloseableIterable;

public class ElasticRequestBuilderTest {

  private DocumentWrapper createWrapper() throws Exception {
    DocumentWrapper result = null;
    AdaptrisMessage adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage("Some Content");
    SimpleDocumentBuilder documentBuilder = new SimpleDocumentBuilder();
    try (CloseableIterable<DocumentWrapper> iterableDocs =
        CloseableIterable.ensureCloseable(documentBuilder.build(adaptrisMessage))) {
        result = iterableDocs.iterator().next();
    }
    return result;
  }

  @Test
  public void testBuildIndexRequest() throws Exception {
    ElasticRequestBuilder builder = new ElasticRequestBuilder();
    DocumentWrapper documentWrapper = createWrapper();
    IndexRequest request = builder.buildIndexRequest("myIndex", documentWrapper, null);
    assertEquals(documentWrapper.uniqueId(), request.id());
    assertEquals("myIndex", request.index());
  }

  @Test
  public void testBuildUpdateRequest() throws Exception {
    ElasticRequestBuilder builder = new ElasticRequestBuilder();
    DocumentWrapper documentWrapper = createWrapper();
    UpdateRequest request = builder.buildUpdateRequest("myIndex", documentWrapper, null);
    assertEquals(documentWrapper.uniqueId(), request.id());
    assertEquals("myIndex", request.index());
  }

  @Test
  public void testBuildUpsertRequest() throws Exception {
    ElasticRequestBuilder builder = new ElasticRequestBuilder();
    DocumentWrapper documentWrapper = createWrapper();
    UpdateRequest request = builder.buildUpsertRequest("myIndex", documentWrapper, null);
    assertEquals(documentWrapper.uniqueId(), request.id());
    assertEquals("myIndex", request.index());
  }

  @Test
  public void testBuildDeleteRequest() throws Exception {
    ElasticRequestBuilder builder = new ElasticRequestBuilder();
    DocumentWrapper documentWrapper = createWrapper();
    DeleteRequest request = builder.buildDeleteRequest("myIndex", documentWrapper, null);
    assertEquals(documentWrapper.uniqueId(), request.id());
    assertEquals("myIndex", request.index());
  }

  @Test
  public void testBuildBulkRequest() throws Exception {
    ElasticRequestBuilder builder = new ElasticRequestBuilder();
    assertNotNull(builder.buildBulkRequest());
  }

}
