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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.apache.commons.lang3.ObjectUtils;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.elastic.DocumentAction;
import com.adaptris.core.elastic.DocumentWrapper;
import com.adaptris.core.elastic.ElasticDocumentBuilder;
import com.adaptris.core.elastic.SimpleDocumentBuilder;
import com.adaptris.core.elastic.actions.ActionExtractor;
import com.adaptris.core.elastic.actions.ConfiguredAction;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.util.CloseableIterable;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * INDEX/UPDATE/DELETE a document(s) to ElasticSearch.
 *
 * <p>
 * {@link ProduceDestination#getDestination(AdaptrisMessage)} should return the index of document that we are submitting to into
 * ElasticSearch; the {@code type} will be derived from the DocumentWrapper itself.
 * </p>
 * <p>
 * Of course, you can configure a {@link ElasticDocumentBuilder} implementation that creates multiple documents, but this will mean
 * that all operations are made individually using the standard single document API rather than the BULK API. For performance
 * reasons you should consider using {@link BulkOperation} where appropriate.
 * </p>
 * <p>
 * The action for each document is driven by the configured {@link ActionExtractor} instance. In the event of an
 * {@link DocumentAction#UPSERT} action then the same {@link XContentBuilder} from the {@link DocumentWrapper} is used as both the
 * update and upsert document via {@code source(XContentBuilder}} and {@code setUpsert(XContentBuilder)}. This makes the assumption
 * that the document generated contains all the data required, not just a subset. If in doubt; stick to a normal
 * {@link DocumentAction#UPDATE} which will correctly throw a {@code DocumentMissingException}.
 * </p>
 *
 *
 * @config elastic-rest-single-operation
 *
 */
@XStreamAlias("elastic-rest-single-operation")
@ComponentProfile(summary = "Use the REST API to interact with an ElasticSearch instance", tag = "producer,elastic,elasticsearch",
    since = "3.9.1", recommended = {ElasticRestConnection.class})
@DisplayOrder(order =
{
    "index", "documentBuilder", "action", "refreshPolicy"
})
public class SingleOperation extends ElasticRestProducer {

  /**
   * How to build the document for elastic.
   * <p>
   * If not explicitly defined then defaults to {@link SimpleDocumentBuilder}
   * </p>
   */
  @Valid
  @NotNull
  @AutoPopulated
  @InputFieldDefault(value = "simple-document-builder")
  @Getter
  @Setter
  @NonNull
  private ElasticDocumentBuilder documentBuilder;

  /**
   * The action for this operation if not explicitly defined by the {@link DocumentWrapper}
   * <p>
   * If not explicitly defined then defaults to {@link ConfiguredAction} with a default of {@code INDEX}.
   * </p>
   */
  @Valid
  @Getter
  @Setter
  @NonNull
  @NotNull
  @InputFieldDefault(value = "configured-action")
  private ActionExtractor action;

  /**
   * The refresh policy
   * <p>
   * This would be generally "true", "false" or "wait_until". The default is null.
   * </p>
   */
  @AdvancedConfig
  @Getter
  @Setter
  private String refreshPolicy;

  protected transient RequestBuilder requestBuilder = new ElasticRequestBuilder();

  public SingleOperation() {
    setDocumentBuilder(new SimpleDocumentBuilder());
  }

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, final String index, long timeout)
      throws ProduceException {
    try {
      TransportClient client = retrieveConnection(TransportClientProvider.class).getTransport();

      try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
        for (DocumentWrapper doc : docs) {
          DocumentAction action =
              ObjectUtils.defaultIfNull(doc.action(), DocumentAction.valueOf(actionExtractor().extract(msg, doc)));

          switch (action) {
            case INDEX: {
              IndexResponse response = client.index(requestBuilder.buildIndexRequest(index, doc, getRefreshPolicy()));
              log.trace("INDEX:: document {} version {} in {}", response.getId(), response.getVersion(), index);
              break;
            }
            case UPDATE: {
              UpdateResponse response = client.update(requestBuilder.buildUpdateRequest(index, doc, getRefreshPolicy()));
              log.trace("UPDATE:: document {} version {} in {}", response.getId(), response.getVersion(), index);
              break;
            }
            case DELETE: {
              DeleteResponse response = client.delete(requestBuilder.buildDeleteRequest(index, doc, getRefreshPolicy()));
              log.trace("DELETE:: document {} version {} in {}", response.getId(), response.getVersion(), index);
              break;
            }
            case UPSERT: {
              UpdateResponse response = client.update(requestBuilder.buildUpsertRequest(index, doc, getRefreshPolicy()));
              log.trace("UPSERT:: document {} version {} in {}", response.getId(), response.getVersion(), index);
              break;
            }
            default:
              throw new ProduceException("Unsupported action: " + action);
          }
        }
      }
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }


  @SuppressWarnings("unchecked")
  public <T extends SingleOperation> T withDocumentBuilder(ElasticDocumentBuilder b) {
    setDocumentBuilder(b);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public <T extends SingleOperation> T withAction(ActionExtractor b) {
    setAction(b);
    return (T) this;
  }


  protected ActionExtractor actionExtractor() {
    return ObjectUtils.defaultIfNull(getAction(), new ConfiguredAction());
  }

  @SuppressWarnings("unchecked")
  public <T extends SingleOperation> T withRefreshPolicy(String b) {
    setRefreshPolicy(b);
    return (T) this;
  }

}
