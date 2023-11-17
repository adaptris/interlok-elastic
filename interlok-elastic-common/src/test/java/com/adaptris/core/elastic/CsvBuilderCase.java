package com.adaptris.core.elastic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.Iterator;

import org.elasticsearch.common.Strings;
import org.junit.jupiter.api.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ProduceException;
import com.adaptris.core.stubs.DefectiveMessageFactory;
import com.adaptris.core.stubs.DefectiveMessageFactory.WhenToBreak;
import com.adaptris.interlok.util.CloseableIterable;
import com.jayway.jsonpath.ReadContext;

public abstract class CsvBuilderCase extends BuilderCase {

  public static final String JSON_PRODUCTUNIQUEID = "$.productuniqueid";
  public static final String CSV_INPUT = "productuniqueid,productname,crop,productcategory,applicationweek,operationdate,manufacturer,applicationrate,measureunit,growthstagecode,iscanonical,latitude,longitude,recordid,id"
      + System.lineSeparator() + "UID-1,*A Simazine,,Insecticides,48,20051122,,1.5,Litres per Hectare,,0,,,5,1" + System.lineSeparator()
      + "UID-2,*Axial,,Herbicides,15,20100408,,0.25,Litres per Hectare,,0,,,6,6" + System.lineSeparator()
      + "UID-3,*Betanal Maxxim,,Herbicides,18,20130501,,0.07,Litres per Hectare,,0,,,21,21" + System.lineSeparator()
      + "UID-4,24-D Amine,Passion Fruit,Herbicides,19,20080506,,2.8,Litres per Hectare,,0,53.37969768091292,-0.18346963126415416,210,209"
      + System.lineSeparator()
      + "UID-5,26N35S,Rape Winter,Fungicides,12,20150314,,200,Kilograms per Hectare,,0,52.71896363632868,-1.2391368098336788,233,217"
      + System.lineSeparator();

  @Test
  public void testBuild() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    msg.addMetadata(getName(), getName());
    CSVDocumentBuilderImpl documentBuilder = createBuilder();
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        ReadContext context = parse(Strings.toString(doc.content()));
        assertEquals("UID-" + count, context.read(JSON_PRODUCTUNIQUEID));
        assertEquals("UID-" + count, doc.uniqueId());
      }
    }
    assertEquals(5, count);
  }

  @Test
  public void testBuild_Invalid_UniqueIdField() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    msg.addMetadata(getName(), getName());
    CSVDocumentBuilderImpl documentBuilder = createBuilder().withUniqueIdField(99);
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      Iterator<?> itr = docs.iterator();
      assertTrue(itr.hasNext());
      assertThrows(RuntimeException.class, () -> itr.next());
    }
  }

  @Test
  public void testBuild_ProduceException() throws Exception {
    AdaptrisMessage msg = new DefectiveMessageFactory(EnumSet.of(WhenToBreak.INPUT, WhenToBreak.OUTPUT)).newMessage(CSV_INPUT);
    msg.addMetadata(getName(), getName());
    CSVDocumentBuilderImpl documentBuilder = createBuilder();
    assertThrows(ProduceException.class, () -> {
      try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      }
    });
  }

  @Test
  public void testBuild_DoubleIterator() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    msg.addMetadata(getName(), getName());
    CSVDocumentBuilderImpl documentBuilder = createBuilder();
    try (CloseableIterable<DocumentWrapper> docs = CloseableIterable.ensureCloseable(documentBuilder.build(msg))) {
      docs.iterator();
      assertThrows(IllegalStateException.class, () -> docs.iterator());
    }
  }

  protected abstract CSVDocumentBuilderImpl createBuilder();

}
