package com.adaptris.core.elastic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import java.io.IOException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Test;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
import com.adaptris.core.elastic.actions.ConfiguredAction;
import com.adaptris.core.elastic.actions.JsonPathAction;
import com.adaptris.core.elastic.actions.MappedAction;
import com.adaptris.core.elastic.actions.MetadataAction;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairList;

public class ActionExtractorTest {

  @Test
  public void testConstantAction() {
    for(DocumentAction val: DocumentAction.values()) {
      ConfiguredAction action = new ConfiguredAction().withAction(val.name());
      assertEquals(val.name(), action.extract(AdaptrisMessageFactory.getDefaultInstance().newMessage(), null));
    }
  }
  
  @Test
  public void testMetadataAction() {
    final String KEY = "my_action";
    MetadataAction action = new MetadataAction();
    action.setMetadataKey(KEY);
    for(DocumentAction val: DocumentAction.values()) {
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
      msg.addMetadata(KEY, val.name());
      assertEquals(val.name(), action.extract(msg, null));
    }
  }
  
  @Test
  public void testJsonPathAction() throws IOException, ServiceException {
    final String FIELD_NAME = "myaction";
    final String JSON_PATH = "$." + FIELD_NAME;
    JsonPathAction action = new JsonPathAction();
    action.setJsonPath(JSON_PATH);
    for (DocumentAction val : DocumentAction.values()) {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      builder.startObject();
      builder.field(FIELD_NAME, val.name());
      builder.endObject();
      assertEquals(val.name(),
          action.extract(AdaptrisMessageFactory.getDefaultInstance().newMessage(), new DocumentWrapper("uid", builder)));
    }
  }
  
  @Test
  public void testMappedAction() throws ServiceException {
    final String KEY = "myaction";
    MappedAction mappedActions = new MappedAction().withAction(new MetadataAction().withKey(KEY));
    MappedAction noMapping = new MappedAction().withAction(new MetadataAction().withKey(KEY)).withMappings(new KeyValuePairList());
    for(DocumentAction val: DocumentAction.values()) {
      mappedActions.getMappings().add(new KeyValuePair(val.name().substring(0, 3), val.name()));
    }
    assertNotNull(mappedActions.getMappings());
    for(DocumentAction val: DocumentAction.values()) {
      AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage();
      String shortName = val.name().substring(0, 3);
      msg.addMetadata(KEY, shortName);
      assertEquals(val, DocumentAction.valueOf(mappedActions.extract(msg, null)));
      assertEquals(shortName, noMapping.extract(msg, null));
    }
  }

}
