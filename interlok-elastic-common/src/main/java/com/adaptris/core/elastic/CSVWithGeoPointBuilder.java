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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.supercsv.io.CsvListReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Builds a document for elastic search.
 *
 * <p>
 * The document that is created contains the following characteristics
 * <ul>
 * <li>The first record of the CSV is assumed to be a header row, and is used as the fieldName for each entry. There is no
 * equivalent of {@code use-header-record} from {@link CSVDocumentBuilder} since we have to know which fields will form the
 * {@code geo_point} fields.</li>
 * <li>The "unique-id" for the document is derived from the specified column, duplicates may have unexpected results depending on
 * your configuration; generally will be an updated version number</li>
 * <li>Any fields which matching "latitude"/"longitude" are aggregated and created as a {@code geo_point} field.
 * </ul>
 * </p>
 *
 * @config elastic-csv-geopoint-document-builder
 *
 */
@XStreamAlias("elastic-csv-geopoint-document-builder")
@ComponentProfile(summary = "Build documents for elasticsearch from a CSV document", since = "3.9.1")
public class CSVWithGeoPointBuilder extends CSVDocumentBuilderImpl {

  /**
   * A comma separated field name that should be considered the latitude field for a geopoint.
   * <p>
   * If not explicitly specified, defaults to {@code latitude,lat}
   * </p>
   */
  @AdvancedConfig
  @InputFieldDefault(value = "latitude,lat")
  @Getter
  @Setter
  private String latitudeFieldNames;

  /**
   * A comma separated field name that should be considered the longitude field for a geopoint.
   * <p>
   * If not explicitly specified, defaults to {@code longitude,lon}
   * </p>
   */
  @AdvancedConfig
  @InputFieldDefault(value = "longitude,lon")
  @Getter
  @Setter
  private String longitudeFieldNames;

  /**
   * The location field name within the elastic document.
   * <p>
   * If not explicitly specified, defaults to {@code location}
   * </p>
   */
  @AdvancedConfig
  @InputFieldDefault(value = "location")
  @Getter
  @Setter
  private String locationFieldName;

  public CSVWithGeoPointBuilder() {
    super();
  }

  @Deprecated
  public CSVWithGeoPointBuilder(FormatBuilder f) {
    super();
    setFormat(f);
  }

  public CSVWithGeoPointBuilder(PreferenceBuilder p) {
    super();
    setPreference(p);
  }

  public CSVWithGeoPointBuilder withLatitudeFieldNames(String s) {
    setLatitudeFieldNames(s);
    return this;
  }

  private String latitudeFieldNames() {
    return ObjectUtils.defaultIfNull(getLatitudeFieldNames(), "latitude,lat");
  }

  public CSVWithGeoPointBuilder withLongitudeFieldNames(String s) {
    setLongitudeFieldNames(s);
    return this;
  }

  private String longitudeFieldNames() {
    return ObjectUtils.defaultIfNull(getLongitudeFieldNames(), "longitude,lon");
  }

  public CSVWithGeoPointBuilder withLocationFieldName(String s) {
    setLocationFieldName(s);
    return this;
  }

  private String locationFieldName() {
    return ObjectUtils.defaultIfNull(getLocationFieldName(), "location");
  }

  @Override
  @Deprecated
  protected CSVDocumentWrapper buildWrapper(CSVParser parser, AdaptrisMessage msg) throws Exception {
    Set<String> latitudeFieldNames = new HashSet<String>(Arrays.asList(latitudeFieldNames().toLowerCase().split(",")));
    Set<String> longitudeFieldNames = new HashSet<String>(Arrays.asList(longitudeFieldNames().toLowerCase().split(",")));
    return new ApacheWrapper(latitudeFieldNames, longitudeFieldNames, parser);
  }

  @Override
  protected CSVDocumentWrapper buildWrapper(CsvListReader reader, AdaptrisMessage message) throws Exception {
    Set<String> latitudeFieldNames = new HashSet<String>(Arrays.asList(latitudeFieldNames().toLowerCase().split(",")));
    Set<String> longitudeFieldNames = new HashSet<String>(Arrays.asList(longitudeFieldNames().toLowerCase().split(",")));
    return new SuperWrapper(latitudeFieldNames, longitudeFieldNames, reader);
  }

  private class SuperWrapper extends CSVDocumentWrapper {
    private String[] headers = new String[0];
    private LatLongHandler latLong;
    private List<String> next;

    public SuperWrapper(Set<String> latitudeFieldNames, Set<String> longitudeFieldNames, CsvListReader reader) {
      super(reader);
      headers = buildHeaders(reader);
      latLong = new LatLongHandler(latitudeFieldNames, longitudeFieldNames, headers);
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
          }
          else {
            throw new Exception("unique-id field > number of fields in record");
          }
          String uniqueId = row.get(idField);
          XContentBuilder builder = jsonBuilder();
          builder.startObject();

          addTimestamp(builder);

          for (int i = 0; i < row.size(); i++) {
            String fieldName = headers[i];
            String data = row.get(i);
            if (!latLong.isLatOrLong(fieldName)) {
              builder.field(getFieldNameMapper().map(fieldName), new Text(data != null ? data : ""));
            }
          }
          latLong.addLatLong(builder, row);
          builder.endObject();
          result = new DocumentWrapper(uniqueId, builder);
        }
      }
      catch (Exception e) {
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
    private LatLongHandler latLong;

    public ApacheWrapper(Set<String> latitudeFieldNames, Set<String> longitudeFieldNames, CSVParser p) {
      super(p);
      headers = buildHeaders(csvIterator.next());
      latLong = new LatLongHandler(latitudeFieldNames, longitudeFieldNames, headers);
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
          String fieldName = headers.get(i);
          String data = record.get(i);
          if (!latLong.isLatOrLong(fieldName)) {
            builder.field(getFieldNameMapper().map(fieldName), new Text(data));
          }
        }
        latLong.addLatLong(builder, record);
        builder.endObject();
        result = new DocumentWrapper(uniqueId, builder);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      return result;
    }
  }

  private class LatLongHandler {

    private final Set<String> latOrLongFieldNames;

    private int lat = -1;
    private int lon = -1;

    LatLongHandler(Set<String> latitudeFieldNames, Set<String> longitudeFieldNames, List<String> headers) {
      latOrLongFieldNames = new HashSet<String>(CollectionUtils.union(latitudeFieldNames, longitudeFieldNames));

      for (int i = 0; i < headers.size(); i++) {
        if (latitudeFieldNames.contains(headers.get(i).toLowerCase())) {
          lat = i;
        }
        if (longitudeFieldNames.contains(headers.get(i).toLowerCase())) {
          lon = i;
        }
      }
    }

    LatLongHandler(Set<String> latitudeFieldNames, Set<String> longitudeFieldNames, String[] headers) {
      latOrLongFieldNames = new HashSet<String>(CollectionUtils.union(latitudeFieldNames, longitudeFieldNames));

      for (int i = 0; i < headers.length; i++) {
        if (latitudeFieldNames.contains(headers[i].toLowerCase())) {
          lat = i;
        }
        if (longitudeFieldNames.contains(headers[i].toLowerCase())) {
          lon = i;
        }
      }
    }

    void addLatLong(XContentBuilder builder, CSVRecord record) throws IOException {
      if (BooleanUtils.or(new boolean[] {lat == -1, lon == -1})) {
        // nothing to do.
        return;
      }
      String latitude = record.get(lat);
      String longitude = record.get(lon);
      try {
        builder.latlon(
            getFieldNameMapper().map(locationFieldName()),
            Double.parseDouble(latitude), Double.parseDouble(longitude));
      }
      catch (NumberFormatException e) {
        // Ignore it, no chance of having a location, because the values aren't real latlongs.
      }
    }

    void addLatLong(XContentBuilder builder, List<String> row) throws IOException {
      if (BooleanUtils.or(new boolean[] {lat == -1, lon == -1})) {
        // nothing to do.
        return;
      }
      String latitude = row.get(lat);
      String longitude = row.get(lon);
      try {
        builder.latlon(
                getFieldNameMapper().map(locationFieldName()),
                Double.parseDouble(latitude), Double.parseDouble(longitude));
      }
      catch (NumberFormatException | NullPointerException e) {
        // Ignore it, no chance of having a location, because the values aren't real latlongs.
      }
    }

    boolean isLatOrLong(String name) {
      return latOrLongFieldNames.contains(name.toLowerCase());
    }
  }
}
