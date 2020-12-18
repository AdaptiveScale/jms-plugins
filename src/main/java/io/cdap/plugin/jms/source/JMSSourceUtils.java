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
import io.cdap.plugin.jms.common.JMSConfig;
import io.cdap.plugin.jms.common.JMSMessageType;
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
                                                      JMSConfig config) {
    Receiver<StructuredRecord> jmsReceiver = new JMSReceiver(StorageLevel.MEMORY_AND_DISK_SER_2(), config);
    return context.getSparkStreamingContext().receiverStream(jmsReceiver);
  }

  public static StructuredRecord convertMessage(Message message, JMSConfig config) throws JMSException,
    IllegalArgumentException {
    if (message instanceof BytesMessage && config.getMessageType().equals(JMSMessageType.BYTES.getName())) {
      return convertByteMessage(message, config);
    }
    if (message instanceof MapMessage && config.getMessageType().equals(JMSMessageType.MAP.getName())) {
      return convertMapMessage(message, config);
    }
    if (message instanceof ObjectMessage && config.getMessageType().equals(JMSMessageType.OBJECT.getName())) {
      return null;
    }
    if (message instanceof Message && config.getMessageType().equals(JMSMessageType.MESSAGE.getName())) {
      return convertPureMessage(message, config);
    }
    if (message instanceof TextMessage && config.getMessageType().equals(JMSMessageType.TEXT.getName())) {
      return convertTextMessage(message, config);
    } else {
      throw new RuntimeException("Message type should be one of Message, Text, Bytes, Map, or Object");
    }
  }

  private static StructuredRecord convertTextMessage(Message message, JMSConfig config) throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord
      .builder(config.getSpecificSchema(config.getMessageType()));
    addHeaderData(recordBuilder, message, config);
    recordBuilder.set("payload", ((TextMessage) message).getText());
    return recordBuilder.build();
  }

  private static StructuredRecord convertByteMessage(Message message, JMSConfig config) throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord
      .builder(config.getSpecificSchema(config.getMessageType()));
    addHeaderData(recordBuilder, message, config);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[8096];
    int currentByte;
    while ((currentByte = ((BytesMessage) message).readBytes(buffer)) != -1) {
      byteArrayOutputStream.write(buffer, 0, currentByte);
    }
    recordBuilder.set("payload", byteArrayOutputStream.toByteArray());
    return recordBuilder.build();
  }

  private static StructuredRecord convertMapMessage(Message message, JMSConfig config) throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord
      .builder(config.getSpecificSchema(config.getMessageType()));
    addHeaderData(recordBuilder, message, config);
    LinkedHashMap<String, Object> mapPayload = new LinkedHashMap<String, Object>();
    Enumeration<String> names = ((MapMessage) message).getMapNames();
    while (names.hasMoreElements()) {
      String key = names.nextElement();
      mapPayload.put(key, ((MapMessage) message).getObject(key));
    }
    recordBuilder.set("payload", mapPayload);
    return recordBuilder.build();
  }

  private static StructuredRecord convertPureMessage(Message message, JMSConfig config) throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord
      .builder(config.getSpecificSchema(config.getMessageType()));
    addHeaderData(recordBuilder, message, config);
    HashMap<String, String> payload = new HashMap<>();
    Enumeration<String> names = ((Message) message).getPropertyNames();
    while (names.hasMoreElements()) {
      String key = (String) names.nextElement();
      payload.put(key, ((Message) message).getStringProperty(key));
    }
    recordBuilder.set("payload", payload);
    return recordBuilder.build();
  }

  private static void addHeaderData(StructuredRecord.Builder recordBuilder, Message message,
                                    JMSConfig config) throws JMSException {
    recordBuilder
      .set(JMSConfig.MESSAGE_ID, Strings.isNullOrEmpty(message.getJMSMessageID()) ? "" : message.getJMSMessageID());
    recordBuilder
      .set(JMSConfig.MESSAGE_TIMESTAMP, message.getJMSTimestamp());
    recordBuilder
      .set(JMSConfig.CORRELATION_ID,
           Strings.isNullOrEmpty(message.getJMSCorrelationID()) ? "" : message.getJMSCorrelationID());
    recordBuilder
      .set(JMSConfig.REPLY_TO, message.getJMSReplyTo() == null ? "" : message.getJMSReplyTo().toString());
    recordBuilder
      .set(JMSConfig.DESTINATION,
           Strings.isNullOrEmpty(message.getJMSDestination().toString()) ? "" : message.getJMSDestination().toString());
    recordBuilder
      .set(JMSConfig.DELIVERY_MODE, message.getJMSDeliveryMode());
    recordBuilder
      .set(JMSConfig.REDELIVERED, message.getJMSRedelivered());
    recordBuilder
      .set(JMSConfig.TYPE, Strings.isNullOrEmpty(message.getJMSType()) ? "" : message.getJMSType());
    recordBuilder
      .set(JMSConfig.EXPIRATION, message.getJMSExpiration());
    recordBuilder
      .set(JMSConfig.PRIORITY, message.getJMSPriority());
  }
}
