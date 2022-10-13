package com.adaptris.core.elastic.sdk.producer;

import javax.validation.constraints.Min;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.elastic.DocumentAction;
import com.adaptris.core.elastic.DocumentWrapper;
import com.adaptris.core.elastic.actions.ActionExtractor;
import com.adaptris.core.elastic.sdk.connection.ElasticConnection;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.util.CloseableIterable;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.util.MissingRequiredPropertyException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Index/Delete/Update a document(s) to ElasticSearch.
 *
 * <p>
 * {@link #getIndex()} should return the index that the documents will be inserted against
 * ElasticSearch; the {@code type} is taken from the DocumentBuilder
 * </p>
 * <p>
 * The action for each document is driven by the configured {@link ActionExtractor} instance. In the
 * event of an {@link DocumentAction#UPSERT} action then the same {@link XContentBuilder} from the
 * {@link DocumentWrapper} is used as both the update and upsert document via
 * {@code doc(XContentBuilder}} and {@code upsert(XContentBuilder)}. This makes the assumption that
 * the document generated contains all the data required, not just a subset. If in doubt; stick to a
 * normal {@link DocumentAction#UPDATE} which will throw a {@code DocumentMissingException} failing
 * the messages.
 * </p>
 *
 * @config elastic-sdk-bulk-operation
 *
 */
@XStreamAlias("elastic-sdk-bulk-operation")
@AdapterComponent
@ComponentProfile(summary = "Use the Elastic SDK to interact with Elasticsearch", tag = "producer,elastic,bulk,batch",
    recommended = {ElasticConnection.class})
@DisplayOrder(order =
{
    "index", "batchWindow", "documentBuilder", "action", "refreshPolicy"
})
@NoArgsConstructor
public class ElasticSdkBulkProducer extends ElasticSdkProducer {

  private static final String DEFAULT_BATCH_WINDOW = "10000";
  /**
   * The batch window which is the number of operations that make a bulk request before its
   * executed.
   * <p>
   * If not specified explicitly then the default is 10000
   * </p>
   */
  @Min(0)
  @InputFieldDefault(value = "10000")
  @Getter
  @Setter
  private String batchWindow;

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, final String index, long timeout) throws ProduceException {
    try {
      ElasticsearchClient client = retrieveConnection(ElasticConnection.class).getClient();
      
      BulkRequest.Builder bulkRequest = new BulkRequest.Builder();
      BulkRequest finalBulkRequest = null;
      
      long total = 0;
      try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(getDocumentBuilder().build(msg))) {
        int count = 0;
        for (DocumentWrapper doc : docs) {
          count++;
          total++;
          DocumentAction action =
              ObjectUtils.defaultIfNull(doc.action(), DocumentAction.valueOf(actionExtractor().extract(msg, doc)));
          switch (action) {
            case INDEX:
              requestBuilder.buildBulkIndexRequest(bulkRequest, index, doc, null, client, msg.getContentEncoding());
              break;
            case UPDATE:
              requestBuilder.buildBulkUpdateRequest(bulkRequest, index, doc, null, client, msg.getContentEncoding());
              break;
            case DELETE:
              requestBuilder.buildBulkDeleteRequest(bulkRequest, index, doc, null);
              break;
            case UPSERT:
              requestBuilder.buildBulkIndexRequest(bulkRequest, index, doc, null, client, msg.getContentEncoding());
              break;
            default:
              throw new ProduceException("Unsupported action: " + action);
          }
          
          if (count >= batchWindow(msg)) {
            doSend(bulkRequest.build(), client);
            count = 0;
            bulkRequest = new BulkRequest.Builder();
            bulkRequest.refresh(getRefreshPolicy() == null ? null : Refresh.valueOf(getRefreshPolicy()));
          }
        }
      }
      try {
        finalBulkRequest = bulkRequest.build();
        if (finalBulkRequest.operations().size() > 0) {
          doSend(finalBulkRequest, client);
        }
      } catch (MissingRequiredPropertyException ex) {
        log.trace("No more operations to send to elastic.");
      }
      log.trace("Produced a total of {} documents", total);
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }

  private void doSend(BulkRequest request, ElasticsearchClient client) throws Exception {
    int count = request.operations().size();
    BulkResponse response = client.bulk(request);
    if (response.errors()) {
      throw new ProduceException(response.toString());
    }
    log.trace("Producing batch of {} requests took {}", count, response.took());
    return;
  }

  public ElasticSdkBulkProducer withBatchWindow(String i) {
    setBatchWindow(i);
    return this;
  }

  private int batchWindow(AdaptrisMessage message) {
    return Integer.parseInt(message.resolve(StringUtils.defaultIfBlank(getBatchWindow(), DEFAULT_BATCH_WINDOW)));
  }
  
}
