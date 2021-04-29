package com.adaptris.core.elastic.csv;

import org.apache.commons.csv.CSVFormat;

/**
 * Builder for creating the required format for parsing the CSV file.
 *
 * <p>
 * Note that this was lifted from the {@code com.adaptris:interlok-csv} project and is now
 * deprecated so switch to using a net.supercsv based implementations instead.
 * </p>
 *
 * @deprecated Use {@link com.adaptris.csv.PreferenceBuilder} instead.
 */
@Deprecated(since = "4.1.0")
public interface FormatBuilder
{

  /**
   * Create the CSVFormat.
   *
   * @return the CSV Format.
   */
  CSVFormat createFormat();
}
