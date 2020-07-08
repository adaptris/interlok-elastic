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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;
import com.adaptris.core.elastic.fields.FieldNameMapper;
import com.adaptris.core.elastic.fields.NoOpFieldNameMapper;
import com.adaptris.core.transform.csv.BasicFormatBuilder;
import com.adaptris.core.transform.csv.FormatBuilder;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.CloseableIterable;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.util.NumberUtils;
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
  @NotNull
  @AutoPopulated
  @Valid
  @Getter
  @Setter
  @NonNull
  private FormatBuilder format;
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

  protected transient Logger log = LoggerFactory.getLogger(this.getClass());

  public CSVDocumentBuilderImpl() {
    setFormat(new BasicFormatBuilder());
    setFieldNameMapper(new NoOpFieldNameMapper());
  }

  @SuppressWarnings("unchecked")
  public <T extends CSVDocumentBuilderImpl> T withFormat(FormatBuilder format) {
    setFormat(format);
    return (T) this;
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

  private String safeName(String input) {
    return defaultIfBlank(input, "").trim().replaceAll(" ", "_");
  }

  @Override
  public Iterable<DocumentWrapper> build(AdaptrisMessage msg) throws ProduceException {
    CSVDocumentWrapper result = null;
    try {
      CSVFormat format = getFormat().createFormat();
      CSVParser parser = format.parse(msg.getReader());
      result = buildWrapper(parser, msg);
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return result;
  }

  protected abstract CSVDocumentWrapper buildWrapper(CSVParser parser, AdaptrisMessage msg) throws Exception;


  protected abstract class CSVDocumentWrapper implements CloseableIterable<DocumentWrapper>, Iterator {
    protected CSVParser parser;
    protected Iterator<CSVRecord> csvIterator;
    private boolean iteratorInvoked = false;

    public CSVDocumentWrapper(CSVParser p) {
      parser = p;
      csvIterator = p.iterator();
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
    @SuppressWarnings("deprecation")
    public void close() throws IOException {
      IOUtils.closeQuietly(parser);
    }

  }
}
