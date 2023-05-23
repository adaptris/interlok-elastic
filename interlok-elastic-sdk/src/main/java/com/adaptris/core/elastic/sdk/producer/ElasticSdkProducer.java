package com.adaptris.core.elastic.sdk.producer;

import java.util.concurrent.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.ObjectUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceException;
import com.adaptris.core.RequestReplyProducerImp;
import com.adaptris.core.elastic.DocumentAction;
import com.adaptris.core.elastic.DocumentWrapper;
import com.adaptris.core.elastic.ElasticDocumentBuilder;
import com.adaptris.core.elastic.SimpleDocumentBuilder;
import com.adaptris.core.elastic.actions.ActionExtractor;
import com.adaptris.core.elastic.actions.ConfiguredAction;
import com.adaptris.core.elastic.sdk.ElasticSdkRequestBuilder;
import com.adaptris.core.elastic.sdk.connection.ElasticConnection;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.interlok.util.CloseableIterable;
import com.adaptris.util.TimeInterval;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.WriteResponseBase;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.UpdateResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * INDEX/UPDATE/DELETE a document(s) to ElasticSearch.
 *
 * <p>
 * {@link #getIndex()} should return the index of document that we are submitting to into ElasticSearch; the {@code type} will be derived
 * from the DocumentWrapper itself.
 * </p>
 * <p>
 * Of course, you can configure a {@link ElasticDocumentBuilder} implementation that creates multiple documents, but this will mean that all
 * operations are made individually using the standard single document API rather than the BULK API. For performance reasons you should
 * consider using {@link BulkOperation} where appropriate.
 * </p>
 * <p>
 * The action for each document is driven by the configured {@link ActionExtractor} instance. In the event of an
 * {@link DocumentAction#UPSERT} action then the same {@link XContentBuilder} from the {@link DocumentWrapper} is used as both the update
 * and upsert document via {@code source(XContentBuilder}} and {@code setUpsert(XContentBuilder)}. This makes the assumption that the
 * document generated contains all the data required, not just a subset. If in doubt; stick to a normal {@link DocumentAction#UPDATE} which
 * will correctly throw a {@code DocumentMissingException}.
 * </p>
 *
 *
 * @config elastic-sdk-single-operation
 *
 */
@XStreamAlias("elastic-sdk-single-operation")
@ComponentProfile(summary = "Use the Elastic SDK to interact with an ElasticSearch instance", tag = "producer,elastic,elasticsearch", since = "4.6", recommended = {
    ElasticConnection.class })
@DisplayOrder(order = { "index", "documentBuilder", "action", "refreshPolicy" })
public class ElasticSdkProducer extends RequestReplyProducerImp {

  private static final TimeInterval TIMEOUT = new TimeInterval(2L, TimeUnit.MINUTES);

  @NotNull
  @Getter
  @Setter
  private String index;

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

  protected transient ElasticSdkRequestBuilder requestBuilder = new ElasticSdkRequestBuilder();

  public ElasticSdkProducer() {
    setDocumentBuilder(new SimpleDocumentBuilder());
  }

  @Override
  protected void doProduce(AdaptrisMessage msg, String endpoint) throws ProduceException {
    doRequest(msg, endpoint, defaultTimeout());
  }

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, final String endpoint, long timeout) throws ProduceException {
    try {
      ElasticsearchClient client = retrieveConnection(ElasticConnection.class).getClient();

      try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
        for (DocumentWrapper doc : docs) {

          DocumentAction action = ObjectUtils.defaultIfNull(doc.action(), DocumentAction.valueOf(actionExtractor().extract(msg, doc)));

          switch (action) {
            case INDEX: {
              IndexResponse response = client
                  .index(requestBuilder.buildIndexRequest(endpoint, doc, getRefreshPolicy(), client, msg.getContentEncoding()));
              log.trace("INDEX:: document {} version {} in {}", response.id(), response.version(), endpoint);
              checkResponse(response);
              break;
            }
            case UPDATE: {
              UpdateResponse response = client.update(
                  requestBuilder.buildUpdateRequest(endpoint, doc, getRefreshPolicy(), client, msg.getContentEncoding()), Void.class);
              log.trace("UPDATE:: document {} version {} in {}", response.id(), response.version(), endpoint);
              checkResponse(response);
              break;
            }
            case DELETE: {
              DeleteResponse response = client.delete(requestBuilder.buildDeleteRequest(endpoint, doc, getRefreshPolicy()));
              log.trace("DELETE:: document {} version {} in {}", response.id(), response.version(), endpoint);
              checkResponse(response);
              break;
            }
            case UPSERT: {
              UpdateResponse response = client.update(
                  requestBuilder.buildUpsertRequest(endpoint, doc, getRefreshPolicy(), client, msg.getContentEncoding()), Void.class);
              log.trace("UPSERT:: document {} version {} in {}", response.id(), response.version(), endpoint);
              checkResponse(response);
              break;
            }
            default:
              throw new ProduceException("Unsupported action: " + action);
          }
        }
      }

    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }

    return msg;
  }

  private void checkResponse(WriteResponseBase response) throws ProduceException {
    if (response.id().isBlank()) {
      throw new ProduceException("Elastic: Could not upsert document.");
    }
  }

  @Override
  protected long defaultTimeout() {
    return TIMEOUT.toMilliseconds();
  }

  protected ActionExtractor actionExtractor() {
    return ObjectUtils.defaultIfNull(getAction(), new ConfiguredAction());
  }

  @Override
  public String endpoint(AdaptrisMessage msg) throws ProduceException {
    return msg.resolve(getIndex());
  }

  @Override
  public void prepare() throws CoreException {
  }

}
