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


//Header - All messages support the same set of header fields. Header fields contain values used by both clients and providers to identify and route messages.
//  Properties - Each message contains a built-in facility for supporting application-defined property values. Properties provide an efficient mechanism for supporting application-defined message filtering.
//  Body - The JMS API defines several types of message body, which cover the majority of messaging styles currently in use.
// Properties allow an application, via message selectors, to have a JMS provider select, or filter, messages on its behalf using application-specific criteria.

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.plugin.jms.config.JMSConfig;
import joptsimple.internal.Strings;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.receiver.Receiver;

import java.util.Properties;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

public class JMSSourceUtils {

  private static final String MESSAGE_ID = "messageId";                 // string
  private static final String MESSAGE_TIMESTAMP = "messageTimestamp";   // long
  private static final String CORRELATION_ID = "correlationId";         // string
  private static final String REPLY_TO = "replyTo";                     // type: Destination
  private static final String DESTINATION = "destination";              // type: Destination
  private static final String DELIVERY_MODE = "deliveryNode";           // int
  private static final String REDELIVERED = "redelivered";              // boolean
  private static final String TYPE = "type";                            // string
  private static final String EXPIRATION = "expiration";                // long
  private static final String PRIORITY = "priority";                    // int

  private static final Schema INITIAL_SCHEMA = Schema.recordOf("message",
                                                               Schema.Field.of(MESSAGE_ID, Schema.of(Schema.Type.STRING)),
                                                               Schema.Field.of(MESSAGE_TIMESTAMP, Schema.of(Schema.Type.LONG)),
                                                               Schema.Field.of(CORRELATION_ID, Schema.of(Schema.Type.STRING)),
                                                               Schema.Field.of(REPLY_TO, Schema.of(Schema.Type.STRING)),
                                                               Schema.Field.of(DESTINATION, Schema.of(Schema.Type.STRING)),
                                                               Schema.Field.of(DELIVERY_MODE, Schema.of(Schema.Type.INT)),
                                                               Schema.Field.of(REDELIVERED, Schema.of(Schema.Type.BOOLEAN)),
                                                               Schema.Field.of(TYPE, Schema.of(Schema.Type.STRING)),
                                                               Schema.Field.of(EXPIRATION, Schema.of(Schema.Type.LONG)),
                                                               Schema.Field.of(PRIORITY, Schema.of(Schema.Type.INT)),
                                                               Schema.Field.of("payload", Schema.of(Schema.Type.STRING)));


  static JavaDStream<StructuredRecord> getJavaDStream(StreamingContext context,
                                                      JMSConfig config) {
    Receiver<StructuredRecord> receiver = JMSSourceUtils.getReceiver(config);
    return context.getSparkStreamingContext().receiverStream(receiver);
  }

  public static Schema getInitialSchema() {
    return INITIAL_SCHEMA;
  }


  // Convert Message to a Structured Record
  public static StructuredRecord convertMessage(Message message) throws JMSException {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(INITIAL_SCHEMA);

    if (message instanceof TextMessage) {
      recordBuilder.set(MESSAGE_ID, Strings.isNullOrEmpty(message.getJMSMessageID()) ? "null" : message.getJMSMessageID());
      recordBuilder.set(MESSAGE_TIMESTAMP, message.getJMSTimestamp());
      recordBuilder.set(CORRELATION_ID, message.getJMSCorrelationID());
      recordBuilder.set(REPLY_TO, message.getJMSReplyTo().toString());
      recordBuilder.set(DESTINATION, message.getJMSDestination().toString());
      recordBuilder.set(DELIVERY_MODE, message.getJMSDeliveryMode());
      recordBuilder.set(REDELIVERED, message.getJMSRedelivered());
      recordBuilder.set(TYPE, message.getJMSType());
      recordBuilder.set(EXPIRATION, message.getJMSExpiration());
      recordBuilder.set(PRIORITY, message.getJMSPriority());
      recordBuilder.set("payload", ((TextMessage) message).getText());


    } else if (message instanceof BytesMessage) {

    } else if (message instanceof ObjectMessage) {

    } else if (message instanceof MapMessage) {

    }

    return recordBuilder.build();

  }

  private static Receiver<StructuredRecord> getReceiver(JMSConfig config) {
    return new Receiver<StructuredRecord>(StorageLevel.MEMORY_ONLY()) {

      private Connection connection;
      @Override
      public StorageLevel storageLevel() {
        return StorageLevel.MEMORY_ONLY();
      }

      @Override
      public void onStart() {
        try {

          Properties properties = new Properties();
          properties.put(Context.INITIAL_CONTEXT_FACTORY, config.getJndiContextFactory().trim());
          properties.put(Context.PROVIDER_URL, config.getProviderUrl().trim());

          Context context = new InitialContext();
          ConnectionFactory factory = (ConnectionFactory) context.lookup(config.getConnectionFactory());
          connection = factory.createConnection(config.getJmsUsername(), config.getJmsPassword());
          Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

          Destination queue = (Destination) context.lookup("MyQueue");

          MessageConsumer messageConsumer = session.createConsumer(queue);
//          messageConsumer.setMessageListener(this);
          connection.start();
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }

      @Override
      public void onStop() {
        try {
          connection.stop();
        }
        catch (JMSException exception) {
          System.out.println(exception);
        }
      }
    };
  }
}
