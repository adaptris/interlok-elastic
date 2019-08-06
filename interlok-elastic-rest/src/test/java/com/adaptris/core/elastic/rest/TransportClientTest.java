package com.adaptris.core.elastic.rest;

import static org.mockito.ArgumentMatchers.any;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;
import org.mockito.Mockito;

public class TransportClientTest {

  @Test
  public void tesIndex() throws Exception {
    RestHighLevelClient restClient = mockClient();
    Sniffer sniffer = Mockito.mock(Sniffer.class);
    try (TransportClient transport = new TransportClient(restClient, sniffer)) {
      IndexRequest request = new IndexRequest();
      transport.index(request);
    }
  }

  @Test
  public void tesUpdate() throws Exception {
    RestHighLevelClient restClient = mockClient();
    Sniffer sniffer = Mockito.mock(Sniffer.class);
    try (TransportClient transport = new TransportClient(restClient, sniffer)) {
      UpdateRequest request = new UpdateRequest();
      transport.update(request);
    }
  }

  @Test
  public void testDelete() throws Exception {
    RestHighLevelClient restClient = mockClient();
    Sniffer sniffer = Mockito.mock(Sniffer.class);
    try (TransportClient transport = new TransportClient(restClient, sniffer)) {
      DeleteRequest request = new DeleteRequest();
      transport.delete(request);
    }
  }

  @Test
  public void testBulk() throws Exception {
    RestHighLevelClient restClient = mockClient();
    Sniffer sniffer = Mockito.mock(Sniffer.class);
    try (TransportClient transport = new TransportClient(restClient, sniffer)) {
      BulkRequest request = new BulkRequest();
      transport.bulk(request);
    }
  }



  private RestHighLevelClient mockClient() throws Exception {
    IndexResponse index = mockIndexResponse();
    UpdateResponse update = mockUpdateResponse();
    DeleteResponse delete = mockDeleteResponse();
    BulkResponse bulk = mockBulkResponse(false);
    RestHighLevelClient client = Mockito.mock(RestHighLevelClient.class);
    Mockito.when(client.index(any(), any())).thenReturn(index);
    Mockito.when(client.update(any(), any())).thenReturn(update);
    Mockito.when(client.delete(any(), any())).thenReturn(delete);
    Mockito.when(client.bulk(any(), any())).thenReturn(bulk);
    return client;
  }


  private IndexResponse mockIndexResponse() {
    IndexResponse response = Mockito.mock(IndexResponse.class);
    Mockito.when(response.getId()).thenReturn("id");
    Mockito.when(response.getVersion()).thenReturn(1L);
    return response;
  }

  private UpdateResponse mockUpdateResponse() {
    UpdateResponse response = Mockito.mock(UpdateResponse.class);
    Mockito.when(response.getId()).thenReturn("id");
    Mockito.when(response.getVersion()).thenReturn(1L);
    return response;
  }

  private DeleteResponse mockDeleteResponse() {
    DeleteResponse response = Mockito.mock(DeleteResponse.class);
    Mockito.when(response.getId()).thenReturn("id");
    Mockito.when(response.getVersion()).thenReturn(1L);
    return response;
  }

  private BulkResponse mockBulkResponse(boolean hasErrors) {
    BulkResponse response = Mockito.mock(BulkResponse.class);

    Mockito.when(response.buildFailureMessage()).thenReturn("We Failed");
    Mockito.when(response.hasFailures()).thenReturn(hasErrors);
    Mockito.when(response.getTook()).thenReturn(new TimeValue(100l));
    return response;
  }

}
