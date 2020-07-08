/*
    Copyright Adaptris Ltd.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.adaptris.core.elastic.rest;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.*;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.elastic.SimpleDocumentBuilder;
import com.adaptris.core.elastic.actions.ConfiguredAction;

public class SingleOperationTest extends ProducerCase {

  private static final String EXAMPLE_COMMENT_HEADER = "\n<!--" + "\n-->\n";
  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }
  @Override
  protected Object retrieveObjectForSampleConfig() {
    ElasticRestConnection esc = new ElasticRestConnection("http://localhost:9200");
    SingleOperation producer = new SingleOperation().withAction(new ConfiguredAction())
        .withDocumentBuilder(new SimpleDocumentBuilder()).withRefreshPolicy(null)
        .withDestination(new ConfiguredProduceDestination("myIndex"));
    return new StandaloneProducer(esc, producer);
  }

  @Override
  protected String getExampleCommentHeader(Object o) {
    return super.getExampleCommentHeader(o) + EXAMPLE_COMMENT_HEADER;
  }

  @Test
  public void testIndex() throws Exception {
    TransportClient client = Mockito.mock(TransportClient.class);
    IndexResponse response = mockIndexResponse();
    Mockito.when(client.index(any())).thenReturn(response);
    ElasticRestConnection conn = new PreConfiguredConnection(client);
    SingleOperation p = createProducerForTests("INDEX");
    StandaloneProducer prod = new StandaloneProducer(conn, p);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello world");
    ServiceCase.execute(prod, msg);
  }


  @Test
  public void testUpdate() throws Exception {
    TransportClient client = Mockito.mock(TransportClient.class);
    UpdateResponse response = mockUpdateResponse();
    Mockito.when(client.update(any())).thenReturn(response);
    ElasticRestConnection conn = new PreConfiguredConnection(client);
    SingleOperation p = createProducerForTests("UPDATE");
    StandaloneProducer prod = new StandaloneProducer(conn, p);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello world");
    ServiceCase.execute(prod, msg);
  }


  @Test
  public void testDelete() throws Exception {
    TransportClient client = Mockito.mock(TransportClient.class);
    DeleteResponse response = mockDeleteResponse();
    Mockito.when(client.delete(any())).thenReturn(response);
    ElasticRestConnection conn = new PreConfiguredConnection(client);
    SingleOperation p = createProducerForTests("DELETE");
    StandaloneProducer prod = new StandaloneProducer(conn, p);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello world");
    ServiceCase.execute(prod, msg);
  }


  @Test
  public void testUpsert() throws Exception {
    TransportClient client = Mockito.mock(TransportClient.class);
    UpdateResponse response = mockUpdateResponse();
    Mockito.when(client.update(any())).thenReturn(response);
    ElasticRestConnection conn = new PreConfiguredConnection(client);
    SingleOperation p = createProducerForTests("UPSERT");
    StandaloneProducer prod = new StandaloneProducer(conn, p);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello world");
    ServiceCase.execute(prod, msg);
  }


  @Test
  public void testService_Exception() throws Exception {
    TransportClient client = Mockito.mock(TransportClient.class);
    ElasticRestConnection conn = new PreConfiguredConnection(client);
    SingleOperation p = createProducerForTests("NONE");
    StandaloneProducer prod = new StandaloneProducer(conn, p);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("hello world");
    try {
      ServiceCase.execute(prod, msg);
      fail();
    } catch (ServiceException expected) {
      
    }
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

  private SingleOperation createProducerForTests(String action) {
    SingleOperation producer = new SingleOperation().withAction(new ConfiguredAction().withAction(action))
        .withDocumentBuilder(new SimpleDocumentBuilder()).withRefreshPolicy(null)
        .withDestination(new ConfiguredProduceDestination("myIndex"));
    return producer;
  }
}
