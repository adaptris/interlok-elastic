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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyObject;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.ServiceException;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.elastic.CSVDocumentBuilder;
import com.adaptris.core.elastic.SimpleDocumentBuilder;
import com.adaptris.core.elastic.actions.ConfiguredAction;

public class BulkOperationTest extends ProducerCase {

  private static final String EXAMPLE_COMMENT_HEADER = "\n<!--" + "\n-->\n";
  public static final String CSV_INPUT =
      "productuniqueid,productname,crop,productcategory,applicationweek,operationdate,manufacturer,applicationrate,measureunit,growthstagecode,iscanonical,latitude,longitude,recordid,id"
          + System.lineSeparator() + "UID-1,*A Simazine,,Insecticides,48,20051122,,1.5,Litres per Hectare,,0,,,5,1"
          + System.lineSeparator() + "UID-2,*Axial,,Herbicides,15,20100408,,0.25,Litres per Hectare,,0,,,6,6"
          + System.lineSeparator() + "UID-3,*Betanal Maxxim,,Herbicides,18,20130501,,0.07,Litres per Hectare,,0,,,21,21"
          + System.lineSeparator()
          + "UID-4,24-D Amine,Passion Fruit,Herbicides,19,20080506,,2.8,Litres per Hectare,,0,53.37969768091292,-0.18346963126415416,210,209"
          + System.lineSeparator()
          + "UID-5,26N35S,Rape Winter,Fungicides,12,20150314,,200,Kilograms per Hectare,,0,52.71896363632868,-1.2391368098336788,233,217"
          + System.lineSeparator();
  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }
  @Test
  public void testRefreshPolicy() throws Exception {
    BulkOperation producer = new BulkOperation();
    assertNull(producer.getRefreshPolicy());
    producer.setRefreshPolicy("wait_until");
    assertEquals("wait_until", producer.getRefreshPolicy());
  }


  @Test
  public void testIndex() throws Exception {
    TransportClient client = Mockito.mock(TransportClient.class);
    BulkResponse response = mockBulkResponse(false);
    Mockito.when(client.bulk(anyObject())).thenReturn(response);
    ElasticRestConnection conn = new PreConfiguredConnection(client);
    BulkOperation p = createProducerForTests("INDEX").withBatchWindow(3);
    StandaloneProducer prod = new StandaloneProducer(conn, p);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    ServiceCase.execute(prod, msg);
  }


  @Test
  public void testUpdate() throws Exception {
    TransportClient client = Mockito.mock(TransportClient.class);
    BulkResponse response = mockBulkResponse(false);
    Mockito.when(client.bulk(anyObject())).thenReturn(response);
    ElasticRestConnection conn = new PreConfiguredConnection(client);
    BulkOperation p = createProducerForTests("UPDATE").withBatchWindow(10);
    StandaloneProducer prod = new StandaloneProducer(conn, p);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    ServiceCase.execute(prod, msg);
  }

  @Test
  public void testDelete() throws Exception {
    TransportClient client = Mockito.mock(TransportClient.class);
    BulkResponse response = mockBulkResponse(false);
    Mockito.when(client.bulk(anyObject())).thenReturn(response);
    ElasticRestConnection conn = new PreConfiguredConnection(client);
    BulkOperation p = createProducerForTests("DELETE").withBatchWindow(1);
    StandaloneProducer prod = new StandaloneProducer(conn, p);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    ServiceCase.execute(prod, msg);
  }

  @Test
  public void testUpsert() throws Exception {
    TransportClient client = Mockito.mock(TransportClient.class);
    BulkResponse response = mockBulkResponse(false);
    Mockito.when(client.bulk(anyObject())).thenReturn(response);
    ElasticRestConnection conn = new PreConfiguredConnection(client);
    BulkOperation p = createProducerForTests("UPSERT").withBatchWindow(1);
    StandaloneProducer prod = new StandaloneProducer(conn, p);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    ServiceCase.execute(prod, msg);
  }

  @Test
  public void testService_BulkHasException() throws Exception {
    TransportClient client = Mockito.mock(TransportClient.class);
    BulkResponse response = mockBulkResponse(true);
    Mockito.when(client.bulk(anyObject())).thenReturn(response);
    ElasticRestConnection conn = new PreConfiguredConnection(client);
    BulkOperation p = createProducerForTests("UPSERT").withBatchWindow(1);
    StandaloneProducer prod = new StandaloneProducer(conn, p);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    try {
      ServiceCase.execute(prod, msg);
      fail();
    } catch (ServiceException expected) {

    }
  }


  @Test
  public void testService_InvalidAction() throws Exception {
    TransportClient client = Mockito.mock(TransportClient.class);
    ElasticRestConnection conn = new PreConfiguredConnection(client);
    BulkOperation p = createProducerForTests("NONE").withBatchWindow(1);
    StandaloneProducer prod = new StandaloneProducer(conn, p);
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    try {
      ServiceCase.execute(prod, msg);
      fail();
    } catch (ServiceException expected) {

    }
  }


  @Override
  protected Object retrieveObjectForSampleConfig() {
    ElasticRestConnection esc = new ElasticRestConnection("http://localhost:9200");
    BulkOperation producer = new BulkOperation().withBatchWindow(1000).withRefreshPolicy(null).withAction(new ConfiguredAction())
        .withDocumentBuilder(new SimpleDocumentBuilder())
        .withDestination(new ConfiguredProduceDestination("myIndex"));
    return new StandaloneProducer(esc, producer);
  }

  @Override
  protected String getExampleCommentHeader(Object o) {
    return super.getExampleCommentHeader(o) + EXAMPLE_COMMENT_HEADER;
  }


  private BulkResponse mockBulkResponse(boolean hasErrors) {
    BulkResponse response = Mockito.mock(BulkResponse.class);

    Mockito.when(response.buildFailureMessage()).thenReturn("We Failed");
    Mockito.when(response.hasFailures()).thenReturn(hasErrors);
    Mockito.when(response.getTook()).thenReturn(new TimeValue(100l));
    return response;
  }

  private BulkOperation createProducerForTests(String action) {
    BulkOperation producer = new BulkOperation().withAction(new ConfiguredAction().withAction(action))
        .withDocumentBuilder(new CSVDocumentBuilder().withUseHeaderRecord(true))
        .withRefreshPolicy(null)
        .withDestination(new ConfiguredProduceDestination("myIndex"));
    return producer;
  }

}
