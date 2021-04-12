package com.adaptris.core.elastic.csv;

import java.lang.reflect.Method;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;

/**
 * Implementation of {@link FormatBuilder} that allows for custom csv formats.
 *
 * <p>
 * Note that this was lifted from the {@code com.adaptris:interlok-csv} project and will eventually
 * be deprecated so that we switch to using a net.supercsv based implementations instead. It is not
 * marked as deprecated just yet.
 * </p>
 *
 * @config csv-custom-format
 *
 */
@XStreamAlias("csv-custom-format")
public class CustomFormatBuilder implements FormatBuilder {

  private transient Logger log = LoggerFactory.getLogger(this.getClass());
  private static final Character COMMA = Character.valueOf(',');
  private static final String DEFAULT_RECORD_SEPARATOR = "\r\n";

  /**
   * The delimiter between each field.
   * <p>
   * defaults to "," if not specified
   * </p>
   */
  @Getter
  @Setter
  private Character delimiter;
  /**
   * Comment support.
   *
   */
  @Getter
  @Setter
  private Character commentStart;
  /**
   * Escape character.
   *
   */
  @Getter
  @Setter
  private Character escape;
  /**
   * Quote character.
   *
   */
  @Getter
  @Setter
  private Character quoteChar;
  /**
   * Whether or not to ignore empty lines.
   * <p>
   * Defaults to false if not specified.
   * </p>
   */
  @Getter
  @Setter
  private Boolean ignoreEmptyLines;
  /**
   * Whether or not to ignore surrounding spaces.
   * <p>
   * Defaults to false if not specified.
   * </p>
   */
  @Getter
  @Setter
  private Boolean ignoreSurroundingSpaces;
  /**
   * Set the record separactor.
   *
   * <p>
   * if not specified defaults to "\r\n"
   * </p>
   */
  @Getter
  @Setter
  private String recordSeparator;

  // These are to protect us mostly from API changes in the nightly build
  // We call the appropriate methods reflectively.
  private static final String[] COMMENT_MARKER_METHODS = {
    "withCommentMarker",
    "withCommentStart"
  };

  private static final String[] QUOTE_CHAR_METHODS =
  {
      "withQuoteChar", "withQuote"
  };

  private static final String[] ESCAPE_CHAR_METHODS =
  {
      "withEscape", "withEscapeChar"
  };

  private static final String[] IGNORE_EMPTY_LINES_METHODS =
  {
    "withIgnoreEmptyLines"
  };

  private static final String[] IGNORE_SURROUNDING_LINES_METHODS =
  {
    "withIgnoreSurroundingSpaces"
  };

  private static final String[] RECORD_SEPARATOR_METHODS =
  {
    "withRecordSeparator"
  };

  private enum FormatOptions {
    COMMENT_MARKER {
      @Override
      public CSVFormat create(CustomFormatBuilder config, CSVFormat current) {
        return configureCSV(current, COMMENT_MARKER_METHODS, Character.class, name(), config.getCommentStart());
      }
    },
    ESCAPE_CHARACTER {
      @Override
      public CSVFormat create(CustomFormatBuilder config, CSVFormat current) {
        return configureCSV(current, ESCAPE_CHAR_METHODS, Character.class, name(), config.getEscape());
      }
    },
    QUOTE_CHARACTER {
      @Override
      public CSVFormat create(CustomFormatBuilder config, CSVFormat current) {
        return configureCSV(current, QUOTE_CHAR_METHODS, Character.class, name(), config.getQuoteChar());
      }

    },
    IGNORE_EMPTY_LINES {
      @Override
      public CSVFormat create(CustomFormatBuilder config, CSVFormat current) {
        return configureCSV(current, IGNORE_EMPTY_LINES_METHODS, boolean.class, name(), config.ignoreEmptyLines());
      }

    },
    IGNORE_SURROUNDING_SPACES {
      @Override
      public CSVFormat create(CustomFormatBuilder config, CSVFormat current) {
        return configureCSV(current, IGNORE_SURROUNDING_LINES_METHODS, boolean.class, name(), config.ignoreSurroundingSpaces());
      }

    },
    RECORD_SEPARATOR {
      @Override
      public CSVFormat create(CustomFormatBuilder config, CSVFormat current) {
        return configureCSV(current, RECORD_SEPARATOR_METHODS, String.class, name(), config.recordSeparator());
      }

    };
    public abstract CSVFormat create(CustomFormatBuilder config, CSVFormat current);
  };

  @Override
  public CSVFormat createFormat() {
    CSVFormat format = CSVFormat.newFormat(delimiter().charValue());
    for (FormatOptions b : FormatOptions.values()) {
      format = b.create(this, format);
    }
    log.trace("CVSFormat created : {}", format);
    return format;
  }

  Character delimiter() {
    return ObjectUtils.defaultIfNull(getDelimiter(), COMMA);
  }

  boolean ignoreEmptyLines() {
    return BooleanUtils.toBooleanDefaultIfNull(getIgnoreEmptyLines(), false);
  }

  boolean ignoreSurroundingSpaces() {
    return BooleanUtils.toBooleanDefaultIfNull(getIgnoreSurroundingSpaces(), false);
  }

  String recordSeparator() {
    return StringUtils.defaultIfEmpty(getRecordSeparator(), DEFAULT_RECORD_SEPARATOR);
  }

  protected static CSVFormat configureCSV(CSVFormat obj, String[] candidates, Class type, String optionName, Object value) {
    CSVFormat result = obj;
    try {
      Method m = findMethod(CSVFormat.class, candidates, type);
      result = (CSVFormat) m.invoke(obj, value);
    }
    catch (Exception e) {
      throw new UnsupportedOperationException("Cannot find appropriate method to handle " + optionName);
    }
    return result;
  }

  private static Method findMethod(Class c, String[] candidates, Class type) {
    Method result = null;
    for (String m : candidates) {
      try {
        result = c.getMethod(m, type);
        break;
      }
      catch (NoSuchMethodException e) {
      }
    }

    return result;
  }
}
