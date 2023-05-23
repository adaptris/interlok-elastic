package com.adaptris.core.elastic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.EnumSet;

import org.junit.jupiter.api.Test;

import com.adaptris.core.CoreException;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JsonSmartJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;

public class JsonHelperTest extends JsonHelper {

  private static final String JSON_OBJECT = "{\"author\":\"J. R. R. Tolkien\",\"price\":22.99,\"isbn\":\"0-395-19395-8\",\"category\":\"fiction\",\"title\":\"The Lord of the Rings\"}";

  private Configuration jsonConfig = new Configuration.ConfigurationBuilder().jsonProvider(new JsonSmartJsonProvider())
      .mappingProvider(new JacksonMappingProvider()).options(EnumSet.noneOf(Option.class)).build();

  protected ReadContext parse(String content) {
    return JsonPath.parse(content, jsonConfig);
  }

  @Test
  public void testGet() throws Exception {
    assertEquals("J. R. R. Tolkien", get(parse(JSON_OBJECT), "$.author"));
  }

  @Test
  public void testGetQuietly() throws Exception {
    ReadContext ctx = parse(JSON_OBJECT);
    assertNull(getQuietly(ctx, ""));
    assertNull(getQuietly(ctx, "$.blahblah"));
    assertEquals("J. R. R. Tolkien", getQuietly(ctx, "$.author"));
  }

  @Test
  public void testWrapAsRuntime() throws Exception {
    assertNotNull(wrapAsRuntimeException(null));
    assertNotNull(wrapAsRuntimeException(new RuntimeException()));
    assertNotNull(wrapAsRuntimeException(new CoreException()));
  }

}
