package com.adaptris.core.elastic.sdk.connection;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("elastic-sdk-noop-authentication")
@AdapterComponent
@ComponentProfile(summary = "Connect to Elasticsearch without authentication.", tag = "auth,elastic")
public class ElasticNoOpAuthenticator extends ElasticAuthenticator {

}
