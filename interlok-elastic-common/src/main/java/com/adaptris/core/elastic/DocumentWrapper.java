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

package com.adaptris.core.elastic;

import org.elasticsearch.common.xcontent.XContentBuilder;

public class DocumentWrapper {

  private final String uniqueId;
  private String routing;
  private DocumentAction action;
  private final XContentBuilder content;
  
  public DocumentWrapper(String uid, XContentBuilder content) {
    this.uniqueId = uid;
    this.content = content;
  }
  
  public XContentBuilder content() {
    return content;
  }

  public String uniqueId() {
    return uniqueId;
  }

  public String routing() {
    return routing;
  }

  public void setRouting(String routing) {
    this.routing = routing;
  }


  public DocumentWrapper withRouting(String r) {
    setRouting(r);
    return this;
  }

  public DocumentWrapper withAction(String s) {
    return withAction(DocumentAction.valueOf(s));
  }

  public DocumentWrapper withAction(DocumentAction a) {
    setAction(a);
    return this;
  }

  /**
   * The action to be taken with this document
   * 
   * @return the action associated with the Document.
   */
  public DocumentAction action() {
    return action;
  }

  public void setAction(DocumentAction action) {
    this.action = action;
  }
  

}
