package com.adaptris.core.elastic.rest;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import com.adaptris.core.elastic.DocumentWrapper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("elastic-request-builder")
public class ElasticRequestBuilder implements RequestBuilder {

  @Override
  public IndexRequest buildIndexRequest(String index, DocumentWrapper doc, String refreshPolicy) {
    return new IndexRequest(index).routing(doc.routing()).id(doc.uniqueId()).setRefreshPolicy(refreshPolicy).source(doc.content());
  }

  @Override
  public UpdateRequest buildUpdateRequest(String index, DocumentWrapper doc, String refreshPolicy) {
    return new UpdateRequest(index, doc.uniqueId()).routing(doc.routing()).setRefreshPolicy(refreshPolicy).doc(doc.content());
  }

  @Override
  public UpdateRequest buildUpsertRequest(String index, DocumentWrapper doc, String refreshPolicy) {
    return buildUpdateRequest(index, doc, refreshPolicy).upsert(doc.content());
  }

  @Override
  public DeleteRequest buildDeleteRequest(String index, DocumentWrapper doc, String refreshPolicy) {
    return new DeleteRequest(index).routing(doc.routing()).id(doc.uniqueId()).setRefreshPolicy(refreshPolicy);
  }

  @Override
  public BulkRequest buildBulkRequest() {
    return new BulkRequest();
  }

}
