package com.adaptris.core.elastic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Date;
import java.util.LinkedHashMap;

import org.elasticsearch.common.Strings;
import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.elastic.csv.BasicFormatBuilder;
import com.adaptris.core.elastic.fields.ToUpperCaseFieldNameMapper;
import com.adaptris.interlok.util.CloseableIterable;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;

@SuppressWarnings("deprecation")
public class CsvGeopointBuilderTest extends CsvBuilderCase {

  private static final String JSON_LOCATION = "$.location";

  public static final String CSV_WITH_LATLON = "productuniqueid,productname,crop,productcategory,applicationweek,operationdate,manufacturer,applicationrate,measureunit,growthstagecode,iscanonical,latitude,longitude,recordid,id"
      + System.lineSeparator()
      + "UID-1,24-D Amine,Passion Fruit,Herbicides,19,20080506,,2.8,Litres per Hectare,,0,53.37969768091292,-0.18346963126415416,210,209"
      + System.lineSeparator()
      + "UID-2,26N35S,Rape Winter,Fungicides,12,20150314,,200,Kilograms per Hectare,,0,52.71896363632868,-1.2391368098336788,233,217"
      + System.lineSeparator();

  public static final String CSV_WITH_LATLON_AND_DELTA = "productuniqueid,productname,crop,productcategory,applicationweek,operationdate,manufacturer,applicationrate,measureunit,growthstagecode,iscanonical,latitude,longitude,recordid,id,delta_status"
      + System.lineSeparator()
      + "UID-1,24-D Amine,Passion Fruit,Herbicides,19,20080506,,2.8,Litres per Hectare,,0,53.37969768091292,-0.18346963126415416,210,209,0"
      + System.lineSeparator()
      + "UID-2,26N35S,Rape Winter,Fungicides,12,20150314,,200,Kilograms per Hectare,,0,52.71896363632868,-1.2391368098336788,233,217,1"
      + System.lineSeparator()
      + "UID-3,26N35S,Rape Winter,Fungicides,12,20150314,,200,Kilograms per Hectare,,0,52.71896363632868,-1.2391368098336788,233,217,2"
      + System.lineSeparator();

  public static final String CSV_WITHOUT_LATLON = "productuniqueid,productname,crop,productcategory,applicationweek,operationdate,manufacturer,applicationrate,measureunit,growthstagecode,iscanonical,latitude,longitude,recordid,id"
      + System.lineSeparator() + "UID-1,*A Simazine,,Insecticides,48,20051122,,1.5,Litres per Hectare,,0,,,5,1" + System.lineSeparator()
      + "UID-2,*Axial,,Herbicides,15,20100408,,0.25,Litres per Hectare,,0,,,6,6" + System.lineSeparator()
      + "UID-3,*Betanal Maxxim,,Herbicides,18,20130501,,0.07,Litres per Hectare,,0,,,21,21" + System.lineSeparator();

  public static final String CSV_NO_LATLON_COLUMNS = "productuniqueid,productname,crop,productcategory,applicationweek,operationdate,manufacturer,applicationrate,measureunit,growthstagecode,iscanonical,recordid,id"
      + System.lineSeparator() + "UID-1,*A Simazine,,Insecticides,48,20051122,,1.5,Litres per Hectare,,0,5,1" + System.lineSeparator()
      + "UID-2,*Axial,,Herbicides,15,20100408,,0.25,Litres per Hectare,,0,6,6" + System.lineSeparator()
      + "UID-3,*Betanal Maxxim,,Herbicides,18,20130501,,0.07,Litres per Hectare,,0,21,21" + System.lineSeparator();

  @Override
  protected CSVWithGeoPointBuilder createBuilder() {
    return new CSVWithGeoPointBuilder().withLatitudeFieldNames("latitude,lat").withLongitudeFieldNames("longitude,lon")
        .withLocationFieldName("location").withFormat(new BasicFormatBuilder());
  }

  @Test
  public void testBuild_WithTimestamp() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_WITH_LATLON);
    CSVWithGeoPointBuilder documentBuilder = new CSVWithGeoPointBuilder().withFormat(new BasicFormatBuilder())
        .withAddTimestampField("My_Timestamp");
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(Strings.toString(doc.content()));
        assertEquals("UID-" + count, context.read(JSON_PRODUCTUNIQUEID));
        assertTrue(Math.abs((Long) context.read("$.My_Timestamp") - new Date().getTime()) < 50);
        LinkedHashMap<?, ?> map = context.read(JSON_LOCATION);
        assertTrue(map.containsKey("lat"));
        assertFalse("0".equals(map.get("lat").toString()));
        assertTrue(map.containsKey("lon"));
        assertFalse("0".equals(map.get("lon").toString()));
      }
    }
    assertEquals(2, count);
  }

  @Test
  public void testBuild_WithLatLong() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_WITH_LATLON);
    CSVWithGeoPointBuilder documentBuilder = new CSVWithGeoPointBuilder().withFormat(new BasicFormatBuilder());
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(Strings.toString(doc.content()));
        assertEquals("UID-" + count, context.read(JSON_PRODUCTUNIQUEID));
        LinkedHashMap<?, ?> map = context.read(JSON_LOCATION);
        assertTrue(map.containsKey("lat"));
        assertFalse("0".equals(map.get("lat").toString()));
        assertTrue(map.containsKey("lon"));
        assertFalse("0".equals(map.get("lon").toString()));
      }
    }
    assertEquals(2, count);
  }

  @Test
  public void testBuild_WithLatLongAndMapper() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_WITH_LATLON);
    CSVWithGeoPointBuilder documentBuilder = new CSVWithGeoPointBuilder().withFormat(new BasicFormatBuilder());
    documentBuilder.setFieldNameMapper(new ToUpperCaseFieldNameMapper());
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(Strings.toString(doc.content()));
        assertEquals("UID-" + count, context.read(JSON_PRODUCTUNIQUEID.toUpperCase()));
        LinkedHashMap<?, ?> map = context.read(JSON_LOCATION.toUpperCase());
        assertTrue(map.containsKey("lat"));
        assertFalse("0".equals(map.get("lat").toString()));
        assertTrue(map.containsKey("lon"));
        assertFalse("0".equals(map.get("lon").toString()));
      }
    }
    assertEquals(2, count);
  }

  @Test
  public void testBuild_WithCustomLatLong() throws Exception {
    String payload = CSV_WITH_LATLON.replace("latitude", "my_lat").replace("longitude", "my_lon");
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(payload);
    CSVWithGeoPointBuilder documentBuilder = new CSVWithGeoPointBuilder().withFormat(new BasicFormatBuilder());
    documentBuilder.setLatitudeFieldNames("My_Lat");
    documentBuilder.setLongitudeFieldNames("My_Lon");
    documentBuilder.setLocationFieldName("My_Location");
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(Strings.toString(doc.content()));
        assertEquals("UID-" + count, context.read(JSON_PRODUCTUNIQUEID));
        LinkedHashMap<?, ?> map = context.read("$.My_Location");
        assertTrue(map.containsKey("lat"));
        assertFalse("0".equals(map.get("lat").toString()));
        assertTrue(map.containsKey("lon"));
        assertFalse("0".equals(map.get("lon").toString()));
      }
    }
    assertEquals(2, count);
  }

  @Test
  public void testBuild_WithLatLongAndDelta() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_WITH_LATLON_AND_DELTA);
    CSVWithGeoPointBuilder documentBuilder = new CSVWithGeoPointBuilder().withFormat(new BasicFormatBuilder());
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(Strings.toString(doc.content()));
        assertEquals("UID-" + count, context.read(JSON_PRODUCTUNIQUEID));
        LinkedHashMap<?, ?> map = context.read(JSON_LOCATION);
        assertTrue(map.containsKey("lat"));
        assertFalse("0".equals(map.get("lat").toString()));
        assertTrue(map.containsKey("lon"));
        assertFalse("0".equals(map.get("lon").toString()));
      }
    }
    assertEquals(3, count);
  }

  @Test
  public void testBuild_WithoutLatLon() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_WITHOUT_LATLON);
    CSVWithGeoPointBuilder documentBuilder = new CSVWithGeoPointBuilder().withFormat(new BasicFormatBuilder());
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(Strings.toString(doc.content()));
        assertEquals("UID-" + count, context.read("$.productuniqueid"));
        try {
          context.read(JSON_LOCATION);
          fail();
        } catch (PathNotFoundException expected) {

        }
      }
    }
    assertEquals(3, count);
  }

  @Test
  public void testBuild_WithoutLatLonHeaders() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_NO_LATLON_COLUMNS);
    CSVWithGeoPointBuilder documentBuilder = new CSVWithGeoPointBuilder().withFormat(new BasicFormatBuilder());
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(Strings.toString(doc.content()));
        assertEquals("UID-" + count, context.read("$.productuniqueid"));
        try {
          context.read(JSON_LOCATION);
          fail();
        } catch (PathNotFoundException expected) {

        }
      }
    }
    assertEquals(3, count);
  }

}
