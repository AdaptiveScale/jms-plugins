package io.cdap.plugin.jms.source;


import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.TextMessage;

import static io.cdap.plugin.jms.source.JMSStreamingSourceConfig.CORRELATION_ID;
import static io.cdap.plugin.jms.source.JMSStreamingSourceConfig.DELIVERY_MODE;
import static io.cdap.plugin.jms.source.JMSStreamingSourceConfig.DESTINATION;
import static io.cdap.plugin.jms.source.JMSStreamingSourceConfig.EXPIRATION;
import static io.cdap.plugin.jms.source.JMSStreamingSourceConfig.MESSAGE_ID;
import static io.cdap.plugin.jms.source.JMSStreamingSourceConfig.MESSAGE_TIMESTAMP;
import static io.cdap.plugin.jms.source.JMSStreamingSourceConfig.PRIORITY;
import static io.cdap.plugin.jms.source.JMSStreamingSourceConfig.REDELIVERED;
import static io.cdap.plugin.jms.source.JMSStreamingSourceConfig.REPLY_TO;
import static io.cdap.plugin.jms.source.JMSStreamingSourceConfig.TYPE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JMSSourceUtilsTest {


  @Test
  public void testConvertTextMessageWithoutHeaders() throws JMSException {
    TextMessage textMessage = mock(TextMessage.class);
    when(textMessage.getText()).thenReturn("Hello World!");

    JMSStreamingSourceConfig config = getJMSStreamingSourceConfig("true");
    StructuredRecord actual = JMSSourceUtils.convertJMSTextMessage(textMessage, config);

    List<Schema.Field> baseSchemaFields =  getExpectedSchemaFields(true);
    baseSchemaFields.add(Schema.Field.of("payload", Schema.of(Schema.Type.STRING)));

    StructuredRecord expected = StructuredRecord
      .builder(Schema.recordOf("message", baseSchemaFields))
      .set("payload", "Hello World!").build();

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testConvertTextMessageWithHeaders() throws JMSException {
    TextMessage textMessage = mock(TextMessage.class);
    when(textMessage.getText()).thenReturn("Hello World!");
    mockHeaders(textMessage);

    JMSStreamingSourceConfig config = getJMSStreamingSourceConfig("false");
    StructuredRecord actual = JMSSourceUtils.convertJMSTextMessage(textMessage, config);

    List<Schema.Field> baseSchemaFields =  getExpectedSchemaFields(false);
    baseSchemaFields.add(Schema.Field.of("payload", Schema.of(Schema.Type.STRING)));

    StructuredRecord.Builder expectedBuilder = StructuredRecord
      .builder(Schema.recordOf("message", baseSchemaFields))
      .set("payload", "Hello World!");
    setExpectedHeaderValues(expectedBuilder);
    StructuredRecord expected = expectedBuilder.build();

    for (Schema.Field field: baseSchemaFields) {
      Assert.assertEquals(expected.get(field.getName()) + "", actual.get(field.getName())+"");
    }
  }

  @Test
  public void testConvertMapMessage() throws JMSException {
    Properties prop = new Properties();
    prop.put("booleanVal", true);
    prop.put("byteVal", (byte) 5);
    prop.put("shortVal", (short) 5);
    prop.put("charVal", 'a');
    prop.put("intVal", 1);
    prop.put("longVal", 1L);
    prop.put("floatVal", 1.0f);
    prop.put("doubleVal", 1.0d);
    prop.put("stringVal", "Hello World!");
    prop.put("bytesVal", "Hello World!".getBytes(StandardCharsets.UTF_8));
    prop.put("objectVal", new Object());

    MapMessage mapMessage = mock(MapMessage.class);
    when(mapMessage.getBoolean("booleanVal")).thenReturn(true);
    when(mapMessage.getByte("byteVal")).thenReturn((byte) 5);
    when(mapMessage.getShort("shortVal")).thenReturn((short) 5);
    when(mapMessage.getChar("charVal")).thenReturn('a');
    when(mapMessage.getInt("intVal")).thenReturn(1);
    when(mapMessage.getLong("longVal")).thenReturn(1L);
    when(mapMessage.getFloat("floatVal")).thenReturn(1.0f);
    when(mapMessage.getDouble("doubleVal")).thenReturn(1.0d);
    when(mapMessage.getString("stringVal")).thenReturn("Hello World!");
    when(mapMessage.getBytes("bytesVal")).thenReturn("Hello World!".getBytes(StandardCharsets.UTF_8));
    when(mapMessage.getObject("objectVal")).thenReturn(new Object());
    when(mapMessage.getMapNames()).thenReturn(prop.propertyNames());


    JMSStreamingSourceConfig config = getJMSStreamingSourceConfig("true");
    StructuredRecord actual = JMSSourceUtils.convertJMSMapMessage(mapMessage, config);
    System.out.println(actual.getSchema());
  }

  @Test
  public void testConvertBytesMessage() {

  }

  @Test
  public void testConvertObjectMessage() {

  }

  @Test
  public void testConvertMessage() {

  }

  private JMSStreamingSourceConfig getJMSStreamingSourceConfig(String removeMessageHeaders) {
    return new JMSStreamingSourceConfig(
      "referenceName", "Connection Factory", "jms-username", "jms-password", "tcp://0.0.0.0:61616", "Queue",
      "jndi-context-factory", "jndi-username", "jndi-password", removeMessageHeaders, "Text", "MyQueue"
    );
  }

  private void mockHeaders(TextMessage message) throws JMSException {
    when(message.getJMSMessageID()).thenReturn("jms-message-id");
    when(message.getJMSCorrelationID()).thenReturn("jms-correlation-id");
    when(message.getJMSReplyTo()).thenReturn(getDummyDestination());
    when(message.getJMSDestination()).thenReturn(getDummyDestination());
    when(message.getJMSType()).thenReturn("jms-type");
    when(message.getJMSTimestamp()).thenReturn(112233445566L);
    when(message.getJMSDeliveryMode()).thenReturn(0);
    when(message.getJMSRedelivered()).thenReturn(false);
    when(message.getJMSExpiration()).thenReturn(112233445566L);
    when(message.getJMSPriority()).thenReturn(0);
  }

  private List<Schema.Field> getExpectedSchemaFields(boolean removeMessageHeaders) {
    List<Schema.Field> baseSchemaFields = new ArrayList<>();

    if (!removeMessageHeaders) {
      baseSchemaFields.addAll(Arrays.asList(
        Schema.Field.of(MESSAGE_ID, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(MESSAGE_TIMESTAMP, Schema.of(Schema.Type.LONG)),
        Schema.Field.of(CORRELATION_ID, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(REPLY_TO, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(DESTINATION, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(DELIVERY_MODE, Schema.of(Schema.Type.INT)),
        Schema.Field.of(REDELIVERED, Schema.of(Schema.Type.BOOLEAN)),
        Schema.Field.of(TYPE, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(EXPIRATION, Schema.of(Schema.Type.LONG)),
        Schema.Field.of(PRIORITY, Schema.of(Schema.Type.INT))
      ));
    }
    return baseSchemaFields;
  }

  private void setExpectedHeaderValues(StructuredRecord.Builder builder) {
    builder.set(MESSAGE_ID, "jms-message-id");
    builder.set(MESSAGE_TIMESTAMP, 112233445566L);
    builder.set(CORRELATION_ID, "jms-correlation-id");
    builder.set(REPLY_TO, getDummyDestination().toString());
    builder.set(DESTINATION, getDummyDestination().toString());
    builder.set(DELIVERY_MODE, 0);
    builder.set(REDELIVERED, false);
    builder.set(TYPE, "jms-type");
    builder.set(EXPIRATION, 112233445566L);
    builder.set(PRIORITY, 0);
  }

  private Destination getDummyDestination() {
    return new Destination() {
      @Override
      public String toString() {
        return "Destination";
      }
    };
  }
}