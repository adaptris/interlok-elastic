package com.adaptris.core.elastic;

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;

public abstract class JsonHelper {

  public static String get(ReadContext ctx, String path) {
    return ctx.read(path);
  }

  public static String getQuietly(ReadContext ctx, String path) {
    String result = null;
    if (isBlank(path)) {
      return null;
    }
    try {
      result = get(ctx, path);
    } catch (PathNotFoundException e) {
      result = null;
    }
    return result;
  }

  public static RuntimeException wrapAsRuntimeException(Exception e) {
    if (e instanceof RuntimeException) {
      return (RuntimeException) e;
    }
    return new RuntimeException(e);
  }
}
