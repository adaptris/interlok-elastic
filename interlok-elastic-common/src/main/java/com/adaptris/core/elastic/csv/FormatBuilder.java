package com.adaptris.core.elastic.csv;

import org.apache.commons.csv.CSVFormat;

/**
 * Builder for creating the required format for parsing the CSV file.
 *
 * <p>
 * Note that this was lifted from the {@code com.adaptris:interlok-csv} project and will eventually
 * be deprecated so that we switch to using a net.supercsv based implementations instead. It is not
 * marked as deprecated just yet.
 * </p>
 */
public interface FormatBuilder {

  /**
   * Create the CSVFormat.
   *
   * @return the CSV Format.
   */
  CSVFormat createFormat();
}
