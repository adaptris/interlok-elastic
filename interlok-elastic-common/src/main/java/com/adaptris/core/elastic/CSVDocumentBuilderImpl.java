/*
    Copyright Adaptris Ltd.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.adaptris.core.elastic;

import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.elastic.csv.BasicFormatBuilder;
import com.adaptris.core.elastic.csv.FormatBuilder;
import com.adaptris.core.elastic.fields.FieldNameMapper;
import com.adaptris.core.elastic.fields.NoOpFieldNameMapper;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.csv.BasicPreferenceBuilder;
import com.adaptris.csv.PreferenceBuilder;
import com.adaptris.interlok.util.CloseableIterable;
import com.adaptris.util.NumberUtils;
import com.adaptris.validation.constraints.ConfigDeprecated;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

public abstract class CSVDocumentBuilderImpl implements ElasticDocumentBuilder {

  /**
   * The format of the CSV file.
   * <p>
   * Defaults to {@link BasicFormatBuilder} by default.
   * </p>
   */
  @AutoPopulated
  @Valid
  @Getter
  @Deprecated
  @ConfigDeprecated(removalVersion = "5.0.0", message = "Use 'preference' instead", groups = Deprecated.class)
  private FormatBuilder format;

  /**
   * The format of the CSV file.
   * <p>
   * Defaults to {@link BasicPreferenceBuilder} by default.
   * </p>
   */
  @AutoPopulated
  // @NotNull
  // @NonNull
  @Valid
  @Getter
  @Setter
  private PreferenceBuilder preference;

  /**
   * Which field in your CSV is considered the unique-id.
   * <p>
   * If not explicitly specified, defaults to the first field (0)
   * </p>
   */
  @AdvancedConfig
  @Min(0)
  @InputFieldDefault(value = "0")
  @Getter
  @Setter
  private Integer uniqueIdField;
  /**
   * Do you have any specific fieldname mapping
   * <p>
   * Defaults to {@link NoOpFieldNameMapper}.
   * </p>
   */
  @AdvancedConfig
  @NotNull
  @Valid
  @Getter
  @Setter
  @NonNull
  private FieldNameMapper fieldNameMapper;
  /**
   * The field which you want to add the ms since epoch to.
   * <p>
   * If not explicitly specified, the timestamp will not be emitted
   * </p>
   */
  @AdvancedConfig
  @Getter
  @Setter
  private String addTimestampField;

  private transient boolean logApacheWarning = false;

  protected transient Logger log = LoggerFactory.getLogger(this.getClass());

  public CSVDocumentBuilderImpl() {
    setFieldNameMapper(new NoOpFieldNameMapper());
  }

  @SuppressWarnings("unchecked")
  @Deprecated
  public <T extends CSVDocumentBuilderImpl> T withFormat(FormatBuilder format) {
    setFormat(format);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public <T extends CSVDocumentBuilderImpl> T withPreferences(PreferenceBuilder preference) {
    setPreference(preference);
    return (T) this;
  }

  @Deprecated
  public void setFormat(FormatBuilder format) {
    this.format = format;
    LoggingHelper.logDeprecation(logApacheWarning, () -> logApacheWarning = true, "format", "preference");
  }

  @SuppressWarnings("unchecked")
  public <T extends CSVDocumentBuilderImpl> T withUniqueIdField(Integer i) {
    setUniqueIdField(i);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public <T extends CSVDocumentBuilderImpl> T withAddTimestampField(String s) {
    setAddTimestampField(s);
    return (T) this;
  }

  protected void addTimestamp(XContentBuilder b) throws IOException {
    if (!isBlank(getAddTimestampField())) {
      b.field(getAddTimestampField(), new Date().getTime());
    }
  }

  protected int uniqueIdField() {
    return NumberUtils.toIntDefaultIfNull(getUniqueIdField(), 0);
  }

  @SuppressWarnings("unchecked")
  public <T extends CSVDocumentBuilderImpl> T withFieldNameMapper(FieldNameMapper s) {
    setFieldNameMapper(s);
    return (T) this;
  }

  protected List<String> buildHeaders(CSVRecord hdrRec) {
    List<String> result = new ArrayList<>();
    for (String hdrValue : hdrRec) {
      result.add(safeName(hdrValue));
    }
    return result;
  }

  protected String[] buildHeaders(CsvListReader mapReader) {
    try {
      return mapReader.getHeader(true);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private String safeName(String input) {
    return defaultIfBlank(input, "").trim().replaceAll(" ", "_");
  }

  @Override
  public Iterable<DocumentWrapper> build(AdaptrisMessage msg) throws ProduceException {
    CSVDocumentWrapper result = null;
    try {

      if (getFormat() == null && getPreference() == null) {
        log.error("");
        throw new IllegalArgumentException("Missing either 'format' or 'preference'");
      }
      if (getFormat() != null && getPreference() != null) {
        log.warn("Do not use both 'format' and 'preference' - defaulting to 'format' for now");
      }

      if (getFormat() != null) {
        CSVFormat format = getFormat().createFormat();
        CSVParser parser = format.parse(msg.getReader());
        result = buildWrapper(parser, msg);
      } else {
        CsvPreference preference = getPreference().build();
        CsvListReader reader = new CsvListReader(msg.getReader(), preference);
        result = buildWrapper(reader, msg);
      }
    } catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return result;
  }

  @Deprecated
  protected abstract CSVDocumentWrapper buildWrapper(CSVParser parser, AdaptrisMessage msg) throws Exception;

  protected abstract CSVDocumentWrapper buildWrapper(CsvListReader reader, AdaptrisMessage message) throws Exception;

  protected static abstract class CSVDocumentWrapper implements CloseableIterable<DocumentWrapper>, Iterator<DocumentWrapper> {
    @Deprecated
    protected CSVParser parser;
    @Deprecated
    protected Iterator<CSVRecord> csvIterator;

    protected CsvListReader reader;

    private boolean iteratorInvoked = false;

    @Deprecated
    public CSVDocumentWrapper(CSVParser p) {
      parser = p;
      csvIterator = p.iterator();
    }

    public CSVDocumentWrapper(CsvListReader r) {
      reader = r;
    }

    @Override
    public Iterator<DocumentWrapper> iterator() {
      if (iteratorInvoked) {
        throw new IllegalStateException("iterator already invoked");
      }
      iteratorInvoked = true;
      return this;
    }

    @Override
    public boolean hasNext() {
      return csvIterator.hasNext();
    }

    @Override
    public void close() throws IOException {
      IOUtils.closeQuietly(parser, reader);
    }
  }

}
