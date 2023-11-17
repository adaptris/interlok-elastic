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
import com.adaptris.core.elastic.JsonArrayDocumentBuilder;
import com.adaptris.core.elastic.actions.ConfiguredAction;
import com.adaptris.core.elastic.sdk.connection.ElasticConnection;
import com.adaptris.core.services.splitter.json.JsonProvider.JsonStyle;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;

public class ElasticSdkBulkProducerTest {

  private AdaptrisMessage message;
  private ElasticSdkBulkProducer producer;

  @Mock
  private ElasticConnection mockConnection;
  @Mock
  private ElasticsearchClient mockClient;
  @Mock
  private ElasticsearchTransport mockTransport;

  @Mock
  private BulkResponse mockBulkResponse;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);

    message = DefaultMessageFactory.getDefaultInstance()
        .newMessage("[\r\n" + "  {\r\n" + "    \"uniqueid\": \"1\",\r\n" + "    \"destination\": \"Some\"\r\n" + "  },\r\n" + "  {\r\n"
            + "    \"uniqueid\": \"2\",\r\n" + "    \"destination\": \"content\"\r\n" + "  },\r\n" + "  {\r\n"
            + "    \"uniqueid\": \"3\",\r\n" + "    \"destination\": \"in\"\r\n" + "  }" + "]");
    message.setContentEncoding("UTF-8");

    ConfiguredAction action = new ConfiguredAction();
    action.setAction("INDEX");

    JsonArrayDocumentBuilder documentBuilder = new JsonArrayDocumentBuilder();
    documentBuilder.setJsonStyle(JsonStyle.JSON_ARRAY);
    documentBuilder.setUniqueIdJsonPath("$.uniqueid");
    documentBuilder.setRoutingJsonPath("$.destination");

    producer = new ElasticSdkBulkProducer();
    producer.setIndex("Index");
    producer.setAction(action);
    producer.setDocumentBuilder(documentBuilder);

    // MOCKS
    producer.registerConnection(mockConnection);
    when(mockConnection.getClient()).thenReturn(mockClient);
    when(mockConnection.retrieveConnection(ElasticConnection.class)).thenReturn(mockConnection);
    when(mockClient.bulk(any(BulkRequest.class))).thenReturn(mockBulkResponse);

    when(mockBulkResponse.errors()).thenReturn(false);

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

    verify(mockClient, times(1)).bulk(any(BulkRequest.class));
  }

  @Test
  public void testBatchWindowIndexDocument() throws Exception {
    producer.withBatchWindow("1");
    producer.produce(message);

    verify(mockClient, times(3)).bulk(any(BulkRequest.class));
  }

  @Test
  public void testUpsertDocument() throws Exception {
    ConfiguredAction action = new ConfiguredAction();
    action.setAction("UPSERT");

    producer.setAction(action);
    producer.produce(message);

    verify(mockClient, times(1)).bulk(any(BulkRequest.class));
  }

  @Test
  public void testDeleteDocument() throws Exception {
    ConfiguredAction action = new ConfiguredAction();
    action.setAction("DELETE");

    producer.setAction(action);
    producer.produce(message);

    verify(mockClient, times(1)).bulk(any(BulkRequest.class));
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
      verify(mockClient, times(0)).bulk(any(BulkRequest.class));
    }
  }

}
