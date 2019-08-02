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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.ObjectUtils;
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
 * Builds a document for elastic search.
 * 
 * <p>
 * The document that is created contains the following characteristics
 * <ul>
 * <li>The first record of the CSV is assumed to be a header row, and is used as the fieldName for each entry</li>
 * <li>The "unique-id" for the document is derived from the specified column, duplicates may have unexpected results depending on
 * your configuration; generally will be an updated version number</li>
 * <li>Any fields which matching "latitude"/"longitude" are aggregated and created as a {@code location} field.
 * </ul>
 * </p>
 * 
 * @config elastic-csv-geopoint-document-builder
 *
 */
@XStreamAlias("elastic-csv-geopoint-document-builder")
@ComponentProfile(summary = "Build documents for elasticsearch from a CSV document", since = "3.9.1")
public class CSVWithGeoPointBuilder extends CSVDocumentBuilderImpl {

  @AdvancedConfig
  @InputFieldDefault(value = "latitude,lat")
  private String latitudeFieldNames;
  
  @AdvancedConfig
  @InputFieldDefault(value = "longitude,lon")
  private String longitudeFieldNames;
  
  @AdvancedConfig
  @InputFieldDefault(value = "location")
  private String locationFieldName;

  public CSVWithGeoPointBuilder() {
    this(new BasicFormatBuilder());
  }

  public CSVWithGeoPointBuilder(FormatBuilder f) {
    super();
    setFormat(f);
  }

  public String getLatitudeFieldNames() {
    return latitudeFieldNames;
  }

  public void setLatitudeFieldNames(String latitudeFieldNames) {
    this.latitudeFieldNames = latitudeFieldNames;
  }

  private String latitudeFieldNames() {
    return ObjectUtils.defaultIfNull(getLatitudeFieldNames(), "latitude,lat");
  }
  
  public String getLongitudeFieldNames() {
    return longitudeFieldNames;
  }

  public void setLongitudeFieldNames(String longitudeFieldNames) {
    this.longitudeFieldNames = longitudeFieldNames;
  }
  
  private String longitudeFieldNames() {
    return ObjectUtils.defaultIfNull(getLongitudeFieldNames(), "longitude,lon");
  }

  public String getLocationFieldName() {
    return locationFieldName;
  }

  public void setLocationFieldName(String locationFieldName) {
    this.locationFieldName = locationFieldName;
  }
  
  private String locationFieldName() {
    return ObjectUtils.defaultIfNull(getLocationFieldName(), "location");
  }
  
  @Override
  protected CSVDocumentWrapper buildWrapper(CSVParser parser, AdaptrisMessage msg) throws Exception {
    Set<String> latitudeFieldNames = new HashSet<String>(Arrays.asList(latitudeFieldNames().toLowerCase().split(",")));
    Set<String> longitudeFieldNames = new HashSet<String>(Arrays.asList(longitudeFieldNames().toLowerCase().split(",")));
    return new MyWrapper(latitudeFieldNames, longitudeFieldNames, parser);
  }

  private class MyWrapper extends CSVDocumentWrapper {
    private List<String> headers = new ArrayList<>();
    private LatLongHandler latLong;

    public MyWrapper(Set<String> latitudeFieldNames, Set<String> longitudeFieldNames, CSVParser p) {
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
          throw new IllegalArgumentException("unique-id field > number of fields in record");
        }
        String uniqueId = record.get(idField);
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        
        addTimestamp(builder);
        
        for (int i = 0; i < record.size(); i++) {
          String fieldName = headers.size() > 0 ? headers.get(i) : "field_" + i;
          String data = record.get(i);
          if (!latLong.isLatOrLong(fieldName)) {
            builder.field(getFieldNameMapper().map(fieldName), new Text(data));
          }
        }
        latLong.addLatLong(builder, record);
        builder.endObject();
        result = new DocumentWrapper(uniqueId, builder);
      }
      catch (IOException e) {
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
      this.latOrLongFieldNames = new HashSet<String>(CollectionUtils.union(latitudeFieldNames, longitudeFieldNames));
      
      for (int i = 0; i < headers.size(); i++) {
        if (latitudeFieldNames.contains(headers.get(i).toLowerCase())) {
          lat = i;
        }
        if (longitudeFieldNames.contains(headers.get(i).toLowerCase())) {
          lon = i;
        }
      }
    }

    void addLatLong(XContentBuilder builder, CSVRecord record) throws IOException {
      if (lat == -1 || lon == -1) {
        return;
      }
      String latitude = record.get(lat);
      String longitude = record.get(lon);
      try {
        builder.latlon(
            getFieldNameMapper().map(locationFieldName()), 
            Double.valueOf(latitude).doubleValue(), Double.valueOf(longitude).doubleValue());
      }
      catch (NumberFormatException e) {
        // Ignore it, no chance of having a location, because the values aren't real latlongs.
      }
    }
    
    boolean isLatOrLong(String name) {
      return latOrLongFieldNames.contains(name.toLowerCase());
    }
  }
}
