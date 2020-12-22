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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.plugin.jms.config.JMSConfig;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.receiver.Receiver;

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
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(config.getSpecificSchema("Text"));

    if (message instanceof TextMessage) {

      String messageId = message.getJMSMessageID();
      recordBuilder.set(JMSConfig.MESSAGE_ID, messageId);
      recordBuilder.set(JMSConfig.MESSAGE_TIMESTAMP, message.getJMSTimestamp());
      recordBuilder.set(JMSConfig.CORRELATION_ID, message.getJMSCorrelationID());
      recordBuilder.set(JMSConfig.REPLY_TO, message.getJMSReplyTo().toString());
      recordBuilder.set(JMSConfig.DESTINATION, message.getJMSDestination().toString());
      recordBuilder.set(JMSConfig.DELIVERY_MODE, message.getJMSDeliveryMode());
      recordBuilder.set(JMSConfig.REDELIVERED, message.getJMSRedelivered());
      recordBuilder.set(JMSConfig.TYPE, message.getJMSType());
      recordBuilder.set(JMSConfig.EXPIRATION, message.getJMSExpiration());
      recordBuilder.set(JMSConfig.PRIORITY, message.getJMSPriority());
      recordBuilder.set("payload", ((TextMessage) message).getText());

    } else if (message instanceof BytesMessage) {

    } else if (message instanceof ObjectMessage) {

    } else if (message instanceof MapMessage) {

    }

    StructuredRecord record = recordBuilder.build();
    return record;
  }

}
