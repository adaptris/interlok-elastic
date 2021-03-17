package com.adaptris.core.elastic.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.apache.commons.csv.CSVFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.adaptris.core.elastic.csv.BasicFormatBuilder.Style;

@SuppressWarnings("deprecation")
public class FormatBuilderTest {

  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testDelimiter() throws Exception {
    CustomFormatBuilder builder = new CustomFormatBuilder();
    assertNull(builder.getDelimiter());
    assertEquals(Character.valueOf(','), builder.delimiter());
    builder.setDelimiter('\t');
    assertEquals(Character.valueOf('\t'), builder.getDelimiter());
    assertEquals(Character.valueOf('\t'), builder.delimiter());
    builder.setDelimiter(null);
    assertNull(builder.getDelimiter());
    assertEquals(Character.valueOf(','), builder.delimiter());
    // Test might fail if they change the API (yet again)
    assertNotNull(builder.createFormat());
    CSVFormat format = builder.createFormat();
    assertEquals(',', format.getDelimiter());
  }

  @Test
  public void testCommentStart() throws Exception {
    CustomFormatBuilder builder = new CustomFormatBuilder();
    assertNull(builder.getCommentStart());
    builder.setCommentStart('a');
    assertEquals(Character.valueOf('a'), builder.getCommentStart());
    assertNotNull(builder.createFormat());
  }

  @Test
  public void testEscape() throws Exception {
    CustomFormatBuilder builder = new CustomFormatBuilder();
    assertNull(builder.getEscape());
    builder.setEscape('a');
    assertEquals(Character.valueOf('a'), builder.getEscape());
    assertNotNull(builder.createFormat());
  }

  @Test
  public void testQuoteChar() throws Exception {
    CustomFormatBuilder builder = new CustomFormatBuilder();
    assertNull(builder.getQuoteChar());
    builder.setQuoteChar('a');
    assertEquals(Character.valueOf('a'), builder.getQuoteChar());
    assertNotNull(builder.createFormat());
  }

  @Test
  public void testIgnoreEmptyLines() throws Exception {
    CustomFormatBuilder builder = new CustomFormatBuilder();
    assertNull(builder.getIgnoreEmptyLines());
    assertFalse(builder.ignoreEmptyLines());

    builder.setIgnoreEmptyLines(Boolean.FALSE);
    assertEquals(Boolean.FALSE, builder.getIgnoreEmptyLines());
    assertFalse(builder.ignoreEmptyLines());

    builder.setIgnoreEmptyLines(null);
    assertNull(builder.getIgnoreEmptyLines());
    assertFalse(builder.ignoreEmptyLines());

    assertNotNull(builder.createFormat());
  }

  @Test
  public void testIgnoreSurroundingSpaces() throws Exception {
    CustomFormatBuilder builder = new CustomFormatBuilder();
    assertNull(builder.getIgnoreSurroundingSpaces());
    assertFalse(builder.ignoreSurroundingSpaces());

    builder.setIgnoreSurroundingSpaces(Boolean.FALSE);
    assertEquals(Boolean.FALSE, builder.getIgnoreSurroundingSpaces());
    assertFalse(builder.ignoreSurroundingSpaces());

    builder.setIgnoreSurroundingSpaces(null);
    assertNull(builder.getIgnoreSurroundingSpaces());
    assertFalse(builder.ignoreSurroundingSpaces());

    assertNotNull(builder.createFormat());
  }

  @Test
  public void testRecordSeparator() throws Exception {
    CustomFormatBuilder builder = new CustomFormatBuilder();
    assertNull(builder.getRecordSeparator());
    assertEquals("\r\n", builder.recordSeparator());
    builder.setRecordSeparator("\n");
    assertEquals("\n", builder.getRecordSeparator());
    assertEquals("\n", builder.recordSeparator());

    builder.setRecordSeparator(null);
    assertNull(builder.getRecordSeparator());
    assertEquals("\r\n", builder.recordSeparator());

    assertNotNull(builder.createFormat());
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testConfigureCSV() throws Exception {
    CSVFormat format = CSVFormat.newFormat(',');
    assertNotNull(CustomFormatBuilder.configureCSV(format, new String[]
    {
        "withCommentMarker", "withCommentStart"
    }, Character.class, "commentMarker", '#'));
    CustomFormatBuilder.configureCSV(format, new String[] {
        "hello",
        "world"
    }, Character.class, "dummyValue", '#');
  }

  @Test
  public void testBasicFormatBuilder() throws Exception {
    for (Style s : Style.values()) {
      assertNotNull(new BasicFormatBuilder(s).createFormat());
    }
  }
}
