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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.BooleanUtils;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.transform.csv.BasicFormatBuilder;
import com.adaptris.core.transform.csv.FormatBuilder;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Builds a simple document for elastic search.
 * 
 * <p>
 * The document that is created contains the following characteristics
 * <ul>
 * <li>The first record of the CSV is assumed to be a header row, and is used as the fieldName for each entry</li>
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

  @AdvancedConfig
  @InputFieldDefault(value = "true")
  private Boolean useHeaderRecord;

  public CSVDocumentBuilder() {
    this(new BasicFormatBuilder());
  }

  public CSVDocumentBuilder(FormatBuilder f) {
    super();
    setFormat(f);
  }

  public Boolean getUseHeaderRecord() {
    return useHeaderRecord;
  }

  /**
   * Whether or not the document contains a header row.
   * 
   * @param b the useHeaderRecord to set, defaults to true.
   */
  public void setUseHeaderRecord(Boolean b) {
    this.useHeaderRecord = b;
  }

  public CSVDocumentBuilder withUseHeaderRecord(Boolean b) {
    setUseHeaderRecord(b);
    return this;
  }

  private boolean useHeaderRecord() {
    return BooleanUtils.toBooleanDefaultIfNull(getUseHeaderRecord(), true);
  }

  @Override
  protected CSVDocumentWrapper buildWrapper(CSVParser parser, AdaptrisMessage msg) throws Exception {
    return new MyWrapper(parser);
  }

  private class MyWrapper extends CSVDocumentWrapper {
    private List<String> headers = new ArrayList<>();

    public MyWrapper(CSVParser p) {
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
          throw new IllegalArgumentException("unique-id field > number of fields in record");
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
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      return result;
    }

  }
}
