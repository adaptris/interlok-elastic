package com.adaptris.core.elastic.sdk.connection;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.interlok.resolver.ExternalResolver;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.security.password.Password;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.Getter;
import lombok.Setter;

@XStreamAlias("elastic-sdk-token-authentication")
@AdapterComponent
@ComponentProfile(summary = "Token based authentication method to connect to Elasticsearch.", tag = "auth,elastic,token")
public class ElasticTokenAuthenticator extends ElasticAuthenticator {

  @Getter
  @Setter
  @InputFieldHint(style = "PASSWORD", external=true)
  private String token;
  
  protected Header[] generateDefaultHeaders() {
    try {
      return new Header[]{
          new BasicHeader("Authorization", "Bearer " + Password.decode(ExternalResolver.resolve(getToken())))
      };
    } catch (PasswordException e) {
      log.error("Could not decode token for elastic token authenticator.", e);
      return new Header[0];
    }
  }
}
