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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.jms.common.JMSConnection;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.naming.Context;

/**
 * This class creates a customized message Receiver and implements the <code>MessageListener</code> interface.
 */
public class JMSReceiver extends Receiver<StructuredRecord> implements MessageListener {
  private static final Logger LOG = LoggerFactory.getLogger(JMSReceiver.class);
  private JMSStreamingSourceConfig config;
  private Connection connection;
  private StorageLevel storageLevel;
  private Session session;
  private JMSConnection jmsConnection;

  public JMSReceiver(StorageLevel storageLevel, JMSStreamingSourceConfig config) {
    super(storageLevel);
    this.storageLevel = storageLevel;
    this.config = config;
  }

  @Override
  public void onStart() {
    this.jmsConnection = new JMSConnection(config);
    Context context = jmsConnection.getContext();
    ConnectionFactory factory = jmsConnection.getConnectionFactory(context);
    connection = jmsConnection.createConnection(factory);
    session = jmsConnection.createSession(connection);
    Destination destination = jmsConnection.getSource(context);
    MessageConsumer messageConsumer = jmsConnection.createConsumer(session, destination);
    jmsConnection.setMessageListener(this, messageConsumer);
    jmsConnection.startConnection(connection);
  }

  @Override
  public void onStop() {
    this.jmsConnection.stopConnection(this.connection);
    this.jmsConnection.closeSession(this.session);
    this.jmsConnection.closeConnection(this.connection);
  }

  @Override
  public void onMessage(Message message) {
    try {
      store(JMSSourceUtils.convertMessage(message, this.config));
    } catch (Exception e) {
      LOG.error("Message couldn't get stored in the Spark memory.", e);
      throw new RuntimeException(e);
    }
  }
}
