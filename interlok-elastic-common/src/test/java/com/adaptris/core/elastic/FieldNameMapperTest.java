package com.adaptris.core.elastic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.adaptris.core.elastic.fields.FieldNameMapper;
import com.adaptris.core.elastic.fields.NoOpFieldNameMapper;
import com.adaptris.core.elastic.fields.ToLowerCaseFieldNameMapper;
import com.adaptris.core.elastic.fields.ToUpperCaseFieldNameMapper;

public class FieldNameMapperTest {
  final String INPUT = "abcABC";

  @Test
  public void testNoOpMapper() {
    FieldNameMapper mapper = new NoOpFieldNameMapper();
    assertEquals(INPUT, mapper.map(INPUT));
  }
  
  @Test
  public void testToUpperCaseMapper() {
    FieldNameMapper mapper = new ToUpperCaseFieldNameMapper();
    assertEquals(INPUT.toUpperCase(), mapper.map(INPUT));
  }

  @Test
  public void testToLowerCaseMapper() {
    FieldNameMapper mapper = new ToLowerCaseFieldNameMapper();
    assertEquals(INPUT.toLowerCase(), mapper.map(INPUT));
  }

}
