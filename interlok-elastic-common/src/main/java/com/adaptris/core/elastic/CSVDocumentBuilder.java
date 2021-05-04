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

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.elastic.csv.FormatBuilder;
import com.adaptris.csv.PreferenceBuilder;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.BooleanUtils;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.supercsv.io.CsvListReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Builds a simple document for elastic search.
 *
 * <p>
 * The document that is created contains the following characteristics
 * <ul>
 * <li>The first record of the CSV is usually assumed to be a header row, and is used as the fieldName for each entry. If
 * {@code use-header-record} is marked as false, then fields will be generated according to their position.</li>
 * <li>The "unique-id" for the document is derived from the specified column, duplicates may have unexpected results depending on
 * your configuration.</li>
 * </ul>
 * </p>
 *
 * @config elastic-csv-document-builder
 *
 */
@XStreamAlias("elastic-csv-document-builder")
@ComponentProfile(summary = "Build documents for elasticsearch from a CSV document", since = "3.9.1")
public class CSVDocumentBuilder extends CSVDocumentBuilderImpl {

  /**
   * Whether or not the document contains a header row.
   *
   * <p>This defaults to true unless otherwise specified</p>
   */
  @AdvancedConfig
  @InputFieldDefault(value = "true")
  @Getter
  @Setter
  private Boolean useHeaderRecord;

  public CSVDocumentBuilder() {
    super();
  }

  @Deprecated
  public CSVDocumentBuilder(FormatBuilder f) {
    super();
    setFormat(f);
  }

  public CSVDocumentBuilder(PreferenceBuilder p) {
    super();
    setPreference(p);
  }

  public CSVDocumentBuilder withUseHeaderRecord(Boolean b) {
    setUseHeaderRecord(b);
    return this;
  }

  private boolean useHeaderRecord() {
    return BooleanUtils.toBooleanDefaultIfNull(getUseHeaderRecord(), true);
  }

  @Override
  @Deprecated
  protected CSVDocumentWrapper buildWrapper(CSVParser parser, AdaptrisMessage msg) {
    return new ApacheWrapper(parser);
  }

  @Override
  protected CSVDocumentWrapper buildWrapper(CsvListReader reader, AdaptrisMessage message) {
    return new SuperWrapper(reader);
  }

  private class SuperWrapper extends CSVDocumentWrapper {
    private String[] headers = new String[0];
    private List<String> next;

    public SuperWrapper(CsvListReader r) {
      super(r);
      if (useHeaderRecord()) {
        headers = buildHeaders(r);
      }
    }

    @Override
    public DocumentWrapper next() {
      DocumentWrapper result = null;
      try {
        List<String> row = next;
        next = null;
        if (row != null) {
          int idField = 0;
          if (uniqueIdField() <= row.size()) {
            idField = uniqueIdField();
          } else {
            throw new Exception("unique-id field > number of fields in record");
          }
          String uniqueId = row.get(idField);
          XContentBuilder builder = jsonBuilder();
          builder.startObject();
          addTimestamp(builder);
          for (int i = 0; i < row.size(); i++) {
            String fieldName = getFieldNameMapper().map(headers.length > 0 ? headers[i] : "field_" + i);
            String data = row.get(i);
            builder.field(fieldName, new Text(data != null ? data : ""));
          }
          builder.endObject();

          result = new DocumentWrapper(uniqueId, builder);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return result;
    }

    @Override
    public boolean hasNext() {
      try {
        next = reader.read();
        return next != null;
      } catch (IOException e) {
        return false;
      }
    }
  }

  @Deprecated
  private class ApacheWrapper extends CSVDocumentWrapper {
    private List<String> headers = new ArrayList<>();

    public ApacheWrapper(CSVParser p) {
      super(p);
      if (useHeaderRecord()) {
        headers = buildHeaders(csvIterator.next());
      }
    }

    @Override
    public DocumentWrapper next() {
      DocumentWrapper result = null;
      try {
        CSVRecord record = csvIterator.next();
        int idField = 0;
        if (uniqueIdField() <= record.size()) {
          idField = uniqueIdField();
        }
        else {
          throw new Exception("unique-id field > number of fields in record");
        }
        String uniqueId = record.get(idField);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        addTimestamp(builder);
        for (int i = 0; i < record.size(); i++) {
          String fieldName = getFieldNameMapper().map(headers.size() > 0 ? headers.get(i) : "field_" + i);
          String data = record.get(i);
          builder.field(fieldName, new Text(data));
        }
        builder.endObject();

        result = new DocumentWrapper(uniqueId, builder);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      return result;
    }

  }
}
