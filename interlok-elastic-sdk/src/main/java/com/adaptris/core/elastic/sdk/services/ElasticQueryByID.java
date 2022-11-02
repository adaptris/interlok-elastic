package com.adaptris.core.elastic.sdk.services;

import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.elastic.sdk.connection.ElasticConnection;
import com.adaptris.core.util.ExceptionHelper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import co.elastic.clients.elasticsearch.core.GetResponse;
import lombok.Getter;
import lombok.Setter;

public class ElasticQueryByID extends ServiceImp {

  @Getter
  @Setter
  private String index;
  @Getter
  @Setter
  private String docId;
  @Getter
  @Setter
  private AdaptrisConnection connection;

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      GetResponse<ObjectNode> response = 
          connection.retrieveConnection(ElasticConnection.class)
            .getClient()
            .get(g -> g.index(msg.resolve(getIndex()))
            .id(msg.resolve(getDocId())), ObjectNode.class);

      if (response.found()) {
        ObjectNode json = response.source();
        msg.setContent(json.toString(), msg.getContentEncoding());
        log.debug("Elastic: Document found with id:" + getDocId());
      } else {
        log.debug("Elastic: Document not found with id: " + getDocId());
      }
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException("Elastic:  Could not query for document", e);
    }
  }

  @Override
  public void start() throws CoreException {
    connection.start();
  }
  
  @Override
  public void stop() {
    connection.stop();
  }
  
  @Override
  public void prepare() throws CoreException {
    connection.prepare();
  }

  @Override
  protected void initService() throws CoreException {
    connection.init();
  }

  @Override
  protected void closeService() {
    connection.close();
  }

}
