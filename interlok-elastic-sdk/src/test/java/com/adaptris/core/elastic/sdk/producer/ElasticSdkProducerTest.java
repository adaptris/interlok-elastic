package com.adaptris.core.elastic.sdk.producer;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.DefaultMessageFactory;
import com.adaptris.core.ProduceException;
import com.adaptris.core.elastic.actions.ConfiguredAction;
import com.adaptris.core.elastic.sdk.connection.ElasticConnection;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.elasticsearch.core.UpdateResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;

public class ElasticSdkProducerTest {

  private AdaptrisMessage message, updateMessage;
  private ElasticSdkProducer producer;

  @Mock
  private ElasticConnection mockConnection;
  @Mock
  private ElasticsearchClient mockClient;
  @Mock
  private ElasticsearchTransport mockTransport;

  @Mock
  private IndexResponse mockIndexResponse;
  @Mock
  private UpdateResponse mockUpdateResponse;
  @Mock
  private DeleteResponse mockDeleteResponse;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);

    message = DefaultMessageFactory.getDefaultInstance()
        .newMessage("{\r\n" + "    \"uniqueid\": \"1\",\r\n" + "    \"destination\": \"content\"\r\n" + "  }");
    message.setContentEncoding("UTF-8");

    updateMessage = DefaultMessageFactory.getDefaultInstance().newMessage("{\r\n" + "\"doc\" : {\r\n" + "    \"test\" : {\r\n"
        + "         \"newTest\" : \"hello\"\r\n" + "        }\r\n" + "    }\r\n" + "}");
    updateMessage.setContentEncoding("UTF-8");

    ConfiguredAction action = new ConfiguredAction();
    action.setAction("INDEX");

    producer = new ElasticSdkProducer();
    producer.setIndex("Index");
    producer.setAction(action);

    // MOCKS
    producer.registerConnection(mockConnection);
    when(mockConnection.getClient()).thenReturn(mockClient);
    when(mockConnection.retrieveConnection(ElasticConnection.class)).thenReturn(mockConnection);
    when(mockClient.index(any(IndexRequest.class))).thenReturn(mockIndexResponse);
    when(mockClient.update(any(UpdateRequest.class), any())).thenReturn(mockUpdateResponse);
    when(mockClient.delete(any(DeleteRequest.class))).thenReturn(mockDeleteResponse);

    when(mockIndexResponse.id()).thenReturn("1");
    when(mockUpdateResponse.id()).thenReturn("1");
    when(mockDeleteResponse.id()).thenReturn("1");

    when(mockClient._transport()).thenReturn(mockTransport);
    when(mockTransport.jsonpMapper()).thenReturn(new JacksonJsonpMapper());
    // MOCKS
  }

  @AfterEach
  public void tearDown() throws Exception {
  }

  @Test
  public void testIndexDocument() throws Exception {
    producer.produce(message);

    verify(mockClient, times(1)).index(any(IndexRequest.class));
  }

  @Test
  public void testUpdateDocument() throws Exception {
    ConfiguredAction action = new ConfiguredAction();
    action.setAction("UPDATE");

    producer.setAction(action);
    producer.produce(updateMessage);

    verify(mockClient, times(1)).update(any(UpdateRequest.class), any());
  }

  @Test
  public void testUpsertDocument() throws Exception {
    ConfiguredAction action = new ConfiguredAction();
    action.setAction("UPSERT");

    producer.setAction(action);
    producer.produce(updateMessage);

    verify(mockClient, times(1)).update(any(UpdateRequest.class), any());
  }

  @Test
  public void testDeleteDocument() throws Exception {
    ConfiguredAction action = new ConfiguredAction();
    action.setAction("DELETE");

    producer.setAction(action);
    producer.produce(message);

    verify(mockClient, times(1)).delete(any(DeleteRequest.class));
  }

  @Test
  public void testNoActionDocument() throws Exception {
    ConfiguredAction action = new ConfiguredAction();
    action.setAction("XXX");

    producer.setAction(action);
    try {
      producer.produce(message);
      fail("Invalid actuion.");
    } catch (ProduceException ex) {
      verify(mockClient, times(0)).delete(any(DeleteRequest.class));
      verify(mockClient, times(0)).update(any(UpdateRequest.class), any());
      verify(mockClient, times(0)).index(any(IndexRequest.class));
    }
  }

}
