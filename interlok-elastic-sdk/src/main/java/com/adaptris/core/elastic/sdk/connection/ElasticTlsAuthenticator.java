package com.adaptris.core.elastic.sdk.connection;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;

import javax.net.ssl.SSLContext;

import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.interlok.resolver.ExternalResolver;
import com.adaptris.security.password.Password;

import lombok.Getter;
import lombok.Setter;

public class ElasticTlsAuthenticator extends ElasticAuthenticator {
  
  @Getter
  @Setter
  private String trustStoreFilePath;
  @Getter
  @Setter
  private String trustStoreType = "pkcs12";
  @Getter
  @Setter
  @InputFieldHint(style = "PASSWORD", external=true)
  private String keyStorePassword;

  protected HttpClientConfigCallback generateAuthenticator() {
    Path trustStorePath;
    try {
      trustStorePath = Paths.get(new URI(getTrustStoreFilePath()));
      KeyStore truststore = KeyStore.getInstance(getTrustStoreType());
      
      try (InputStream is = Files.newInputStream(trustStorePath)) {
          truststore.load(is, Password.decode(ExternalResolver.resolve(keyStorePassword)).toCharArray());
      }
      SSLContextBuilder sslBuilder = SSLContexts.custom().loadTrustMaterial(truststore, null);
      final SSLContext sslContext = sslBuilder.build();
      
      return new HttpClientConfigCallback() {
        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
          return httpClientBuilder.setSSLContext(sslContext);
        }
      };
    } catch (Exception e) {
      log.error("Could not generate TLS credentials for elastic TLS authenticator.", e);
      return super.generateAuthenticator();
    }
    
  }
  
}
