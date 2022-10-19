package com.adaptris.core.elastic.sdk.connection;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

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

@XStreamAlias("elastic-sdk-apikey-authentication")
@AdapterComponent
@ComponentProfile(summary = "Use an API key as your authentication method to connect to Elasticsearch.", tag = "auth,elastic,api,apikey")
public class ElasticApiKeyAuthenticator extends ElasticAuthenticator {

  @Getter
  @Setter
  @InputFieldHint(style = "PASSWORD", external=true)
  private String keyId;
  @Getter
  @Setter
  @InputFieldHint(style = "PASSWORD", external=true)
  private String keySecret;
  
  protected Header[] generateDefaultHeaders() {
    try {
      String apiKeyAuth = Base64.getEncoder().encodeToString(
              (Password.decode(ExternalResolver.resolve(getKeyId())) + ":" + 
                  Password.decode(ExternalResolver.resolve(getKeySecret())))
                    .getBytes(StandardCharsets.UTF_8));
      
      return new Header[] {new BasicHeader("Authorization", "ApiKey " + apiKeyAuth)};
    } catch (PasswordException e) {
      log.error("Could not decode token for elastic token authenticator.", e);
      return new Header[0];
    }
  }
  
}
