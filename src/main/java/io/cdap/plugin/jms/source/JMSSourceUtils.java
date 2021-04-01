/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.jms.source;

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.plugin.jms.common.JMSMessageType;
import org.apache.commons.lang.SerializationUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.receiver.Receiver;
import java.io.ByteArrayOutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

/**
 * An utils class that encapsulates the necessary functionality to convert different types of JMS messages to
 * StructuredRecords, and creates an input stream with our customized implementation of receiver {@link JMSReceiver}.
 */
public class JMSSourceUtils {

  static JavaDStream<StructuredRecord> getJavaDStream(StreamingContext context,
                                                      JMSStreamingSourceConfig config) {
    Receiver<StructuredRecord> jmsReceiver = new JMSReceiver(StorageLevel.MEMORY_AND_DISK_SER_2(), config);
    return context.getSparkStreamingContext().receiverStream(jmsReceiver);
  }

  /**
   * Creates a {@link StructuredRecord} from a JMS message.
   *
   * @param message the incoming JMS message
   * @param config the {@link JMSStreamingSourceConfig} with all user provided property values
   * @return the {@link StructuredRecord} built out of the JMS message fields
   * @throws JMSException in case the method fails to read fields from the JMS message
   * @throws IllegalArgumentException in case the user provides a non-supported message type
   */
  public static StructuredRecord convertMessage(Message message, JMSStreamingSourceConfig config)
    throws JMSException, IllegalArgumentException {
    String messageType = null;
    if (!Strings.isNullOrEmpty(config.getMessageType())) {
      messageType = config.getMessageType();
    } else {
      throw new RuntimeException("Message type should not be null.");
    }

    if (message instanceof BytesMessage && messageType.equals(JMSMessageType.BYTES)) {
      return convertJMSByteMessage(message, config);
    }
    if (message instanceof MapMessage && messageType.equals(JMSMessageType.MAP)) {
      return convertJMSMapMessage(message, config);
    }
    if (message instanceof ObjectMessage && messageType.equals(JMSMessageType.OBJECT)) {
      return convertJMSObjectMessage(message, config);
    }
    if (message instanceof Message && messageType.equals(JMSMessageType.MESSAGE)) {
      return convertJMSMessage(message, config);
    }
    if (message instanceof TextMessage && messageType.equals(JMSMessageType.TEXT)) {
      return convertJMSTextMessage(message, config);
    } else {
      throw new IllegalArgumentException("Message type should be one of Message, Text, Bytes, Map, or Object");
    }
  }

  /**
   * Creates a {@link StructuredRecord} from a JMS {@link TextMessage}.
   *
   * @param message the incoming JMS {@link TextMessage}
   * @param config the {@link JMSStreamingSourceConfig} with all user provided property values
   * @return the {@link StructuredRecord} built out of the JMS {@link TextMessage} fields
   * @throws JMSException in case the method fails to read fields from the JMS message
   */
   public static StructuredRecord convertJMSTextMessage(Message message, JMSStreamingSourceConfig config)
    throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord
      .builder(config.getSpecificSchema(config.getMessageType(), config.getRemoveMessageHeaders()));
    if (!config.getRemoveMessageHeaders()) {
      addHeaderData(recordBuilder, message);
    }
    recordBuilder.set("payload", ((TextMessage) message).getText());
    return recordBuilder.build();
  }

  /**
   * Creates a {@link StructuredRecord} from a JMS {@link MapMessage}.
   *
   * @param message the incoming JMS {@link MapMessage}
   * @param config the {@link JMSStreamingSourceConfig} with all user provided property values
   * @return the {@link StructuredRecord} built out of the JMS {@link MapMessage} fields
   * @throws JMSException in case the method fails to read fields from the JMS message
   */
  public static StructuredRecord convertJMSMapMessage(Message message, JMSStreamingSourceConfig config)
    throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord
      .builder(config.getSpecificSchema(config.getMessageType(), config.getRemoveMessageHeaders()));
    if (!config.getRemoveMessageHeaders()) {
      addHeaderData(recordBuilder, message);
    }
    LinkedHashMap<String, Object> mapPayload = new LinkedHashMap<String, Object>();
    Enumeration<String> names = ((MapMessage) message).getMapNames();
    while (names.hasMoreElements()) {
      String key = names.nextElement();
      mapPayload.put(key, ((MapMessage) message).getObject(key));
    }
    recordBuilder.set("payload", mapPayload);
    return recordBuilder.build();
  }

  /**
   * Creates a {@link StructuredRecord} from a JMS {@link BytesMessage}.
   *
   * @param message the incoming JMS {@link BytesMessage}
   * @param config the {@link JMSStreamingSourceConfig} with all user provided property values
   * @return the {@link StructuredRecord} built out of the JMS {@link BytesMessage} fields
   * @throws JMSException in case the method fails to read fields from the JMS message
   */
  public static StructuredRecord convertJMSByteMessage(Message message, JMSStreamingSourceConfig config)
    throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord
      .builder(config.getSpecificSchema(config.getMessageType(), config.getRemoveMessageHeaders()));
    if (!config.getRemoveMessageHeaders()) {
      addHeaderData(recordBuilder, message);
    }
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[8096];
    int currentByte;
    while ((currentByte = ((BytesMessage) message).readBytes(buffer)) != -1) {
      byteArrayOutputStream.write(buffer, 0, currentByte);
    }
    recordBuilder.set("payload", byteArrayOutputStream.toByteArray());
    return recordBuilder.build();
  }

  /**
   * Creates a {@link StructuredRecord} from a JMS {@link ObjectMessage}.
   *
   * @param message the incoming JMS {@link ObjectMessage}
   * @param config the {@link JMSStreamingSourceConfig} with all user provided property values
   * @return the {@link StructuredRecord} built out of the JMS {@link ObjectMessage} fields
   * @throws JMSException in case the method fails to read fields from the JMS message
   */
  public static StructuredRecord convertJMSObjectMessage(Message message, JMSStreamingSourceConfig config)
    throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord
      .builder(config.getSpecificSchema(config.getMessageType(), config.getRemoveMessageHeaders()));
    if (!config.getRemoveMessageHeaders()) {
      addHeaderData(recordBuilder, message);
    }
    byte[] payload = SerializationUtils.serialize(((ObjectMessage) message).getObject());
    recordBuilder.set("payload", payload);
    return recordBuilder.build();
  }

  /**
   * Creates a {@link StructuredRecord} from a JMS {@link Message}.
   *
   * @param message the incoming JMS {@link Message}
   * @param config the {@link JMSStreamingSourceConfig} with all user provided property values
   * @return the {@link StructuredRecord} built out of the JMS {@link Message} fields
   * @throws JMSException in case the method fails to read fields from the JMS message
   */
  public static StructuredRecord convertJMSMessage(Message message, JMSStreamingSourceConfig config)
    throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord
      .builder(config.getSpecificSchema(config.getMessageType(), config.getRemoveMessageHeaders()));
    if (!config.getRemoveMessageHeaders()) {
      addHeaderData(recordBuilder, message);
    }
    HashMap<String, String> payload = new HashMap<>();
    Enumeration<String> names = ((Message) message).getPropertyNames();
    while (names.hasMoreElements()) {
      String key = (String) names.nextElement();
      payload.put(key, ((Message) message).getStringProperty(key));
    }
    recordBuilder.set("payload", payload);
    return recordBuilder.build();
  }

  /**
   * Gets header data fields from the JMS message and adds them to the passed record builder.
   *
   * @param recordBuilder the {@link StructuredRecord.Builder} we insert header data into
   * @param message the incoming JMS message
   * @throws JMSException in case the method fails to read fields from the JMS message
   */
  private static void addHeaderData(StructuredRecord.Builder recordBuilder, Message message) throws JMSException {

    if (!Strings.isNullOrEmpty(message.getJMSMessageID())) {
      recordBuilder.set(JMSStreamingSourceConfig.MESSAGE_ID, message.getJMSMessageID());
    }
    if (!Strings.isNullOrEmpty(message.getJMSCorrelationID())) {
      recordBuilder.set(JMSStreamingSourceConfig.CORRELATION_ID, message.getJMSCorrelationID());
    }
    if (message.getJMSReplyTo() != null) {
      recordBuilder.set(JMSStreamingSourceConfig.REPLY_TO, message.getJMSReplyTo().toString());
    }
    if (message.getJMSDestination() != null) {
      recordBuilder.set(JMSStreamingSourceConfig.DESTINATION, message.getJMSDestination().toString());
    }
    if (!Strings.isNullOrEmpty(message.getJMSType())) {
      recordBuilder.set(JMSStreamingSourceConfig.TYPE, message.getJMSType());
    }
    recordBuilder.set(JMSStreamingSourceConfig.MESSAGE_TIMESTAMP, message.getJMSTimestamp());
    recordBuilder.set(JMSStreamingSourceConfig.DELIVERY_MODE, message.getJMSDeliveryMode());
    recordBuilder.set(JMSStreamingSourceConfig.REDELIVERED, message.getJMSRedelivered());
    recordBuilder.set(JMSStreamingSourceConfig.EXPIRATION, message.getJMSExpiration());
    recordBuilder.set(JMSStreamingSourceConfig.PRIORITY, message.getJMSPriority());
  }
}
