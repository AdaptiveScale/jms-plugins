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

package io.cdap.plugin.jms.sink;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.plugin.jms.common.JMSConnection;
import io.cdap.plugin.jms.common.JMSMessageType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;

/**
 * Record writer to produce messages to a JMS Topic/Queue.
 */
public class JMSRecordWriter extends RecordWriter<NullWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(JMSRecordWriter.class);
  private static final Gson GSON = new GsonBuilder().create();

  private final JMSBatchSinkConfig jmsConfig;
  private Connection connection;
  private Session session;
  private MessageProducer messageProducer;
  private JMSConnection jmsConnection;

  public JMSRecordWriter(TaskAttemptContext context) {
    Configuration config = context.getConfiguration();
    String configJson = config.get(JMSOutputFormatProvider.PROPERTY_CONFIG_JSON);
    jmsConfig = GSON.fromJson(configJson, JMSBatchSinkConfig.class);
    this.jmsConnection = new JMSConnection(jmsConfig);
    establishConnection();
  }

  @Override
  public void write(NullWritable key, Text text) {
    try {
      if (jmsConfig.getMessageType().equals(JMSMessageType.MAP)) {
        HashMap<String, Object> map = GSON.fromJson(text.toString(), new HashMap<String, Object>().getClass());
        MapMessage mapMessage = session.createMapMessage();
        map.forEach((k, v) -> {
          try {
            mapMessage.setObject(k, v);
          } catch (JMSException e) {
            LOG.error("Couldn't set field {} at the map message!", k);
            throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
          }
        });
        produceMessage(mapMessage);
      } else {
        TextMessage textMessage = session.createTextMessage();
        textMessage.setText(text.toString());
        produceMessage(textMessage);
      }
    } catch (JMSException e) {
      LOG.error("Message couldn't get written!");
      throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) {
    this.jmsConnection.stopConnection(this.connection);
    this.jmsConnection.closeSession(this.session);
    this.jmsConnection.closeConnection(this.connection);
  }

  private void produceMessage(Message message) throws JMSException {
    messageProducer.send(message);
  }

  private void establishConnection() {
    Context context = jmsConnection.getContext();
    ConnectionFactory factory = jmsConnection.getConnectionFactory(context);
    connection = jmsConnection.createConnection(factory);
    session = jmsConnection.createSession(connection);
    Destination destination = jmsConnection.getSink(context, session);
    messageProducer = jmsConnection.createProducer(session, destination);
  }
}
