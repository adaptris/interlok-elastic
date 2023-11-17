package com.adaptris.core.elastic.sdk;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.elastic.DocumentWrapper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.JsonpMapper;
import jakarta.json.spi.JsonProvider;

@XStreamAlias("elastic-sdk-request-builder")
public class ElasticSdkRequestBuilder {
  
  protected transient Logger log = LoggerFactory.getLogger(this.getClass().getName());

  public IndexRequest<JsonData> buildIndexRequest(String index, DocumentWrapper doc, String refreshPolicy, ElasticsearchClient client, String charEnc) {
    return IndexRequest.of(eIndex -> {
        return eIndex
            .index(index)
            .withJson(new ByteArrayInputStream(Strings.toString(doc.content()).getBytes(Charset.forName(charEnc))))
            .routing(doc.routing())
            .id(doc.uniqueId())
            .refresh(refreshPolicy == null ? null : Refresh.valueOf(refreshPolicy));
    });
  }

  public UpdateRequest buildUpdateRequest(String index, DocumentWrapper doc, String refreshPolicy, ElasticsearchClient client, String charEnc, boolean upsert) {
    return UpdateRequest.of(eIndex -> {
      return eIndex
          .index(index)
          .doc(Strings.toString(doc.content()).getBytes(Charset.forName(charEnc)))
          .routing(doc.routing())
          .id(doc.uniqueId())
          .docAsUpsert(upsert)
          .refresh(refreshPolicy == null ? null : Refresh.valueOf(refreshPolicy));
    });
  }
  
  public UpdateRequest buildUpdateRequest(String index, DocumentWrapper doc, String refreshPolicy, ElasticsearchClient client, String charEnc) {
    return buildUpdateRequest(index, doc, refreshPolicy, client, charEnc, false);
  }

  public UpdateRequest buildUpsertRequest(String index, DocumentWrapper doc, String refreshPolicy, ElasticsearchClient client, String charEnc) {
    return buildUpdateRequest(index, doc, refreshPolicy, client, charEnc, true);
  }

  public DeleteRequest buildDeleteRequest(String index, DocumentWrapper doc, String refreshPolicy) {
    return DeleteRequest.of(eIndex -> {
      return eIndex
          .index(index)
          .routing(doc.routing())
          .id(doc.uniqueId())
          .refresh(refreshPolicy == null ? null : Refresh.valueOf(refreshPolicy));
    });
  }

  public void buildBulkIndexRequest(BulkRequest.Builder bulkRequest, String index, DocumentWrapper doc, String refreshPolicy, ElasticsearchClient client, String charEnc) {
    log.trace("Indexing content: " + Strings.toString(doc.content()));
    bulkRequest.operations(op -> op.index(eIndex -> eIndex
        .index(index)
        .document(readJson(
            new ByteArrayInputStream(
                Strings.toString(doc.content()).getBytes(Charset.forName(charEnc))), client))
        .routing(doc.routing())
        .id(doc.uniqueId())
      ));
  }
  
  public void buildBulkUpdateRequest(BulkRequest.Builder bulkRequest, String index, DocumentWrapper doc, String refreshPolicy, ElasticsearchClient client, String charEnc) {    
    bulkRequest.operations(op -> op.update(eIndex -> eIndex
        .index(index)
        .withJson(new ByteArrayInputStream(readJson(
            new ByteArrayInputStream(
                Strings.toString(doc.content()).getBytes(Charset.forName(charEnc))), client).toString().getBytes(Charset.forName(charEnc))
            ))
        .routing(doc.routing())
        .id(doc.uniqueId())
      ));
  }
  
  public void buildBulkDeleteRequest(BulkRequest.Builder bulkRequest, String index, DocumentWrapper doc, String refreshPolicy) {
    bulkRequest.operations(op -> op.delete(eIndex -> eIndex
        .index(index)
        .routing(doc.routing())
        .id(doc.uniqueId())
      ));
  }

  public static JsonData readJson(InputStream input, ElasticsearchClient esClient) {
    JsonpMapper jsonpMapper = esClient._transport().jsonpMapper();
    JsonProvider jsonProvider = jsonpMapper.jsonProvider();

    return JsonData.from(jsonProvider.createParser(input), jsonpMapper);
  }
  
}
  
