package com.adaptris.core.elastic.sdk.connection;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

import javax.net.ssl.SSLContext;

import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldHint;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.Getter;
import lombok.Setter;

/**
 * Sets up the client to trust the CA that has signed the certificate that 
 * Elasticsearch is using, when that CA certificate is available as a PEM encoded file.
 * 
 * @author Aaron
 *
 */
@XStreamAlias("elastic-sdk-trustedtls-authentication")
@AdapterComponent
@ComponentProfile(summary = "Always trust TLS authentication method to connect to Elasticsearch.", tag = "auth,elastic,tls,trusted")
public class ElasticTrustedTlsAuthenticator extends ElasticAuthenticator {
  
  @Getter
  @Setter
  private String caCertFilePath;
  @Getter
  @Setter
  private String certType = "X.509";
  @Getter
  @Setter
  private String keyStoreType = "pkcs12";
  @Getter
  @Setter
  @InputFieldHint(style = "PASSWORD", external=true)
  private String keyStorePassword;

  protected HttpClientConfigCallback generateAuthenticator() {
    try {
      Path caCertificatePath = Paths.get(new URI(getCaCertFilePath()));
      CertificateFactory factory = CertificateFactory.getInstance(getCertType());
      Certificate trustedCa;
      try (InputStream is = Files.newInputStream(caCertificatePath)) {
          trustedCa = factory.generateCertificate(is);
      }
      KeyStore trustStore = KeyStore.getInstance(keyStoreType);
      trustStore.load(null, null);
      trustStore.setCertificateEntry("ca", trustedCa);
      SSLContextBuilder sslContextBuilder = SSLContexts.custom().loadTrustMaterial(trustStore, null);
      final SSLContext sslContext = sslContextBuilder.build();
      
      return new HttpClientConfigCallback() {
        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
          return httpClientBuilder.setSSLContext(sslContext);
        }
      };
    } catch (Exception ex) {
      log.error("Could not generate TLS credentials for elastic TLS authenticator.", ex);
      return super.generateAuthenticator();
    } 
  }
}
