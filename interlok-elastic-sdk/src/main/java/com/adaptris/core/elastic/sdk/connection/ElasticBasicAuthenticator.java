package com.adaptris.core.elastic.sdk.connection;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.interlok.resolver.ExternalResolver;
import com.adaptris.security.exc.PasswordException;
import com.adaptris.security.password.Password;

import lombok.Getter;
import lombok.Setter;

public class ElasticBasicAuthenticator extends ElasticAuthenticator {
  
  @Getter
  @Setter
  private String username;
  @Getter
  @Setter
  @InputFieldHint(style = "PASSWORD", external=true)
  private String password;

  protected HttpClientConfigCallback generateAuthenticator() {
    
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    try {
      credentialsProvider.setCredentials(AuthScope.ANY, 
          new UsernamePasswordCredentials(getUsername(), 
              Password.decode(ExternalResolver.resolve(getPassword()))));
    } catch (PasswordException e) {
      log.error("Could not decode password for basic elastic authenticator.", e);
    }
    
    return new HttpClientConfigCallback() {
      @Override
      public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
          return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      }
    };
  }
  
  
}
