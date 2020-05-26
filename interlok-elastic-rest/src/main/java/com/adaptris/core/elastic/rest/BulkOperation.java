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

import javax.validation.constraints.Min;
import org.apache.commons.lang3.ObjectUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.elastic.DocumentAction;
import com.adaptris.core.elastic.DocumentWrapper;
import com.adaptris.core.elastic.actions.ActionExtractor;
import com.adaptris.core.util.CloseableIterable;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.util.NumberUtils;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Index/Delete/Update a document(s) to ElasticSearch.
 * 
 * <p>
 * {@link ProduceDestination#getDestination(AdaptrisMessage)} should return the index that the documents will be inserted against
 * ElasticSearch; the {@code type} is taken from the DocumentBuilder
 * </p>
 * <p>
 * The action for each document is driven by the configured {@link ActionExtractor} instance. In the event of an
 * {@link DocumentAction#UPSERT} action then the same {@link XContentBuilder} from the {@link DocumentWrapper} is used as both the
 * update and upsert document via {@code doc(XContentBuilder}} and {@code upsert(XContentBuilder)}. This makes the assumption
 * that the document generated contains all the data required, not just a subset. If in doubt; stick to a normal
 * {@link DocumentAction#UPDATE} which will throw a {@code DocumentMissingException} failing the messages.
 * </p>
 * 
 * @config elastic-rest-bulk-operation
 *
 */
@XStreamAlias("elastic-rest-bulk-operation")
@AdapterComponent
@ComponentProfile(summary = "Use the REST API to interact with Elasticsearch", tag = "producer,elastic,bulk,batch",
    recommended = {ElasticRestConnection.class})
@DisplayOrder(order =
{
    "batchWindow", "documentBuilder", "action", "refreshPolicy"
})
@NoArgsConstructor
public class BulkOperation extends SingleOperation {

  private static final int DEFAULT_BATCH_WINDOW = 10000;
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
  private Integer batchWindow;

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, ProduceDestination destination, long timeout) throws ProduceException {
    try {
      TransportClient client = retrieveConnection(TransportClientProvider.class).getTransport();
      final String index = destination.getDestination(msg);
      BulkRequest bulkRequest = requestBuilder.buildBulkRequest().setRefreshPolicy(getRefreshPolicy());
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
              bulkRequest.add(requestBuilder.buildIndexRequest(index, doc, null));
              break;
            case UPDATE:
              bulkRequest.add(requestBuilder.buildUpdateRequest(index, doc, null));
              break;
            case DELETE:
              bulkRequest.add(requestBuilder.buildDeleteRequest(index, doc, null));
              break;
            case UPSERT:
              bulkRequest.add(requestBuilder.buildUpsertRequest(index, doc, null));
              break;
            default:
              throw new ProduceException("Unsupported action: " + action);
          }
          if (count >= batchWindow()) {
            doSend(bulkRequest, client);
            count = 0;
            bulkRequest = requestBuilder.buildBulkRequest().setRefreshPolicy(getRefreshPolicy());
          }
        }
      }
      if (bulkRequest.numberOfActions() > 0) {
        doSend(bulkRequest, client);
      }
      log.trace("Produced a total of {} documents", total);
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }

  private void doSend(BulkRequest request, TransportClient client) throws Exception {
    int count = request.numberOfActions();
    BulkResponse response = client.bulk(request);
    if (response.hasFailures()) {
      throw new ProduceException(response.buildFailureMessage());
    }
    log.trace("Producing batch of {} requests took {}", count, response.getTook().toString());
    return;
  }

  public BulkOperation withBatchWindow(Integer i) {
    setBatchWindow(i);
    return this;
  }

  private int batchWindow() {
    return NumberUtils.toIntDefaultIfNull(getBatchWindow(), DEFAULT_BATCH_WINDOW);
  }

}
