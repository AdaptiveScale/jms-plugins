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
import io.cdap.plugin.jms.common.JMSConfig;
import io.cdap.plugin.jms.common.JMSConnection;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.naming.Context;

public class JMSReceiver extends Receiver<StructuredRecord> implements MessageListener {
  private static final String RECORDS_UPDATED_METRIC = "records.updated";

  private static Logger logger = LoggerFactory.getLogger(JMSReceiver.class);
  private JMSConfig config;
  private StorageLevel storageLevel;
  private Connection connection;
  private Session session;
  private JMSConnection jmsConnection;
  private StreamingContext streamingContext;

  public JMSReceiver(JMSConfig config, StorageLevel storageLevel) {
    super(storageLevel);
    this.storageLevel = storageLevel;
    this.config = config;
  }

  public void setStreamingContext(StreamingContext streamingContext) {
    this.streamingContext = streamingContext;
  }

  @Override
  public void onStart() {

    this.jmsConnection = new JMSConnection(config);

    Context context = jmsConnection.getContext();

    ConnectionFactory factory = jmsConnection.getConnectionFactory(context);

    connection = jmsConnection.createConnection(factory);

    session = jmsConnection.createSession(connection);

    Destination destination = jmsConnection.getDestination(context);

    MessageConsumer messageConsumer = jmsConnection.createConsumer(session, destination);

    jmsConnection.setMessageListener(this, messageConsumer);

    jmsConnection.startConnection(connection);
  }

  @Override
  public void onStop() {
    try {
      connection.stop();
      session.close();
      connection.close();
    } catch (JMSException e) {
      logger.error("Connection couldn't be stopped.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onMessage(Message message) {
    try {
      store(JMSSourceUtils.convertMessage(message, this.config));
      recordMetric(this.streamingContext);
    } catch (Exception e) {
      logger.error("Message couldn't be stored in spark memory.", e);
      throw new RuntimeException(e);
    }
  }

  private void recordMetric(StreamingContext context) {
    context.getMetrics().count(RECORDS_UPDATED_METRIC, 1);
  }
}
