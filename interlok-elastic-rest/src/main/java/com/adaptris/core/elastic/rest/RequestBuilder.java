package com.adaptris.core.elastic.rest;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import com.adaptris.core.elastic.DocumentWrapper;

public interface RequestBuilder {

  public IndexRequest buildIndexRequest(String index, DocumentWrapper doc, String refreshPolicy);
  
  public UpdateRequest buildUpdateRequest(String index, DocumentWrapper doc, String refreshPolicy);

  public UpdateRequest buildUpsertRequest(String index, DocumentWrapper doc, String refreshPolicy);
  
  public DeleteRequest buildDeleteRequest(String index, DocumentWrapper doc, String refreshPolicy);
  
  public BulkRequest buildBulkRequest();
  
}
