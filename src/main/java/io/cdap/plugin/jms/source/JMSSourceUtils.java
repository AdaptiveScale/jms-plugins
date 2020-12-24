package io.cdap.plugin.jms.source;

/*
 * Copyright © 2016-2020 Cask Data, Inc.
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

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.plugin.jms.common.JMSConfig;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.receiver.Receiver;

import java.util.Enumeration;
import java.util.HashMap;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

public class JMSSourceUtils {

  static JavaDStream<StructuredRecord> getJavaDStream(StreamingContext context,
                                                      JMSConfig config) {
    Receiver<StructuredRecord> JMSReceiver = new JMSReceiver(StorageLevel.MEMORY_AND_DISK_SER_2(), config);
    return context.getSparkStreamingContext().receiverStream(JMSReceiver);
  }

  public static StructuredRecord convertMessage(Message message, JMSConfig config) throws JMSException {
    if (message instanceof TextMessage) {
      return convertTextMessage(message, config);
    } else if (message instanceof BytesMessage) {
      return convertByteMessage(message, config);
    } else if (message instanceof MapMessage) {
      return convertMapMessage(message, config);
    } else if (message instanceof ObjectMessage) {
      return null;
    } else if (message instanceof Message) {
      return convertPureMessage(message, config);
    }
    return null;
  }

  private static StructuredRecord convertTextMessage(Message message, JMSConfig config) throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(config.getSpecificSchema(config.getMessageType()));
    addHeaderData(recordBuilder, message, config);
    recordBuilder.set("payload", ((TextMessage) message).getText());
    return recordBuilder.build();
  }

  private static StructuredRecord convertByteMessage(Message message, JMSConfig config) throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(config.getSpecificSchema(config.getMessageType()));
    addHeaderData(recordBuilder, message, config);
    int byteLength = (int) ((BytesMessage) message).getBodyLength();
    byte[] bytes = new byte[byteLength];
    for (int i = 0; i < byteLength; i++) {
      bytes[i] = ((BytesMessage) message).readByte();
    }
    recordBuilder.set("payload", bytes);
    return recordBuilder.build();
  }

  private static StructuredRecord convertMapMessage(Message message, JMSConfig config) throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(config.getSpecificSchema(config.getMessageType()));
    addHeaderData(recordBuilder, message, config);
    HashMap<String, String> m = new HashMap<>();
    Enumeration<?> it = ((MapMessage) message).getMapNames();
    while (it.hasMoreElements()) {
      String name = (String) it.nextElement();
      m.put(name, ((MapMessage) message).getString(name));
    }
    recordBuilder.set("payload", m);
    return recordBuilder.build();
  }

  private static StructuredRecord convertPureMessage(Message message, JMSConfig config) throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(config.getSpecificSchema(config.getMessageType()));
    addHeaderData(recordBuilder, message, config);
    // todo: we need to handle this like we did with the map
    return recordBuilder.build();
  }

  private static StructuredRecord.Builder addHeaderData(StructuredRecord.Builder recordBuilder, Message message,
                                                        JMSConfig config) throws JMSException {
    recordBuilder.set(JMSConfig.MESSAGE_ID, Strings.isNullOrEmpty(message.getJMSMessageID()) ? "" : message.getJMSMessageID());
    recordBuilder.set(JMSConfig.MESSAGE_TIMESTAMP, message.getJMSTimestamp());
    recordBuilder.set(JMSConfig.CORRELATION_ID, Strings.isNullOrEmpty(message.getJMSCorrelationID()) ? "" : message.getJMSCorrelationID());
    recordBuilder.set(JMSConfig.REPLY_TO, message.getJMSReplyTo() == null ? "" : message.getJMSReplyTo().toString());
    recordBuilder.set(JMSConfig.DESTINATION, Strings.isNullOrEmpty(message.getJMSDestination().toString()) ? "" : message.getJMSDestination().toString());
    recordBuilder.set(JMSConfig.DELIVERY_MODE, message.getJMSDeliveryMode());
    recordBuilder.set(JMSConfig.REDELIVERED, message.getJMSRedelivered());
    recordBuilder.set(JMSConfig.TYPE, Strings.isNullOrEmpty(message.getJMSType()) ? "" : message.getJMSType());
    recordBuilder.set(JMSConfig.EXPIRATION, message.getJMSExpiration());
    recordBuilder.set(JMSConfig.PRIORITY, message.getJMSPriority());
    return recordBuilder;
  }
}
