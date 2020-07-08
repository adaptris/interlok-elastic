package com.adaptris.core.elastic.rest;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TransportClient implements Closeable {

  private transient RestHighLevelClient restHighLevelClient;
  private transient Sniffer sniffer;

  @Override
  @SuppressWarnings("deprecation")
  public void close() {
    IOUtils.closeQuietly(sniffer());
    IOUtils.closeQuietly(restClient());
  }

  public RestHighLevelClient restClient() {
    return restHighLevelClient;
  }

  public Sniffer sniffer() {
    return sniffer;
  }

  public IndexResponse index(IndexRequest request) throws IOException {
    return restClient().index(request, RequestOptions.DEFAULT);
  }

  public BulkResponse bulk(BulkRequest bulkRequest) throws IOException {
    return restClient().bulk(bulkRequest, RequestOptions.DEFAULT);
  }

  public UpdateResponse update(UpdateRequest request) throws IOException {
    return restClient().update(request, RequestOptions.DEFAULT);
  }

  public DeleteResponse delete(DeleteRequest request) throws IOException {
    return restClient().delete(request, RequestOptions.DEFAULT);
  }
}
