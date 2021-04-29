package com.adaptris.core.elastic.csv;

import com.adaptris.annotation.AutoPopulated;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.csv.CSVFormat;

import javax.validation.constraints.NotNull;

/**
 * Implementation of {@link FormatBuilder} that maps the standard commons-csv formats.
 *
 * <p>
 * Basic parsing options are supported : {@link BasicFormatBuilder.Style#DEFAULT DEFAULT},
 * {@link BasicFormatBuilder.Style#EXCEL EXCEL}, {@link BasicFormatBuilder.Style#RFC4180 RFC4180},
 * {@link BasicFormatBuilder.Style#MYSQL MYSQL} and {@link BasicFormatBuilder.Style#TAB_DELIMITED
 * TAB DELIMITED} which correspond to the base formats defined by {@link CSVFormat}.
 * </p>
 * <p>
 * Note that this was lifted from the {@code com.adaptris:interlok-csv} project and will eventually
 * be deprecated so that we switch to using a net.supercsv based implementations instead. It is not
 * marked as deprecated just yet.
 * </p>
 *
 * @config csv-basic-format
 */
@XStreamAlias("csv-basic-format")
public class BasicFormatBuilder implements FormatBuilder {

  /**
   * enum representing the standard supported CSV Formats.
   *
   * @see CSVFormat
   */
  public enum Style {
    /**
     * Corresponds to {@link CSVFormat#DEFAULT} which is equivalent to RFC4180 but ignores empty lines.
     * <ul>
     * <li>withDelimiter(',')</li>
     * <li>withQuoteChar('"')</li>
     * <li>withRecordSeparator(CRLF)</li>
     * <li>withIgnoreEmptyLines(true)</li>
     * </ul>
     */
    DEFAULT {
      @Override
      CSVFormat createFormat() {
        return CSVFormat.DEFAULT;
      }
    },
    /**
     * Corresponds to {@link CSVFormat#EXCEL}.
     * <p>
     * Note that the actual value delimiter used by Excel is locale dependent, so this defaults to the baseline format rather than
     * using the locale specific format.
     * </p>
     * <ul>
     * <li>withDelimiter(',')</li>
     * <li>withQuoteChar('"')</li>
     * <li>withRecordSeparator(CRLF)</li>
     * </ul>
     */
    EXCEL {
      @Override
      CSVFormat createFormat() {
        return CSVFormat.EXCEL;
      }
    },
    /**
     * Corresponds to {@link CSVFormat#MYSQL}.
     *
     * <p>
     * Default MySQL format used by the <tt>SELECT INTO OUTFILE</tt> and <tt>LOAD DATA INFILE</tt> operations.
     * </p>
     * <ul>
     * <li>withDelimiter(TAB)</li>
     * <li>withEscape(BACKSLASH)</li>
     * <li>withRecordSeparator(LF)</li>
     * </ul>
     */
    MYSQL {
      @Override
      CSVFormat createFormat() {
        return CSVFormat.MYSQL;
      }
    },
    /**
     * Corresponds to {@link CSVFormat#RFC4180}.
     * <ul>
     * <li>withDelimiter(',')</li>
     * <li>withQuoteChar('"')</li>
     * <li>withRecordSeparator(CRLF)</li>
     * </ul>
     */
    RFC4180 {
      @Override
      CSVFormat createFormat() {
        return CSVFormat.RFC4180;
      }
    },
    /**
     * Corresponds to {@link CSVFormat#TDF}.
     * <ul>
     * <li>withDelimiter(TAB)</li>
     * <li>withIgnoreSurroundingSpaces(true)</li>
     * </ul>
     */
    TAB_DELIMITED {
      @Override
      CSVFormat createFormat() {
        return CSVFormat.TDF;
      }
    };

    abstract CSVFormat createFormat();
  };

  /**
   * The Style of CSV that you wish to use.
   *
   */
  @NotNull
  @AutoPopulated
  @Getter
  @Setter
  private Style style;

  public BasicFormatBuilder() {
    this(Style.DEFAULT);
  }

  public BasicFormatBuilder(Style style) {
    setStyle(style);
  }

  @Override
  public CSVFormat createFormat() {
    return getStyle().createFormat();
  }
}
