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

package io.cdap.plugin.jms.common;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * A facade class that encapsulates the necessary functionality to: get the initial context, resolve the connection
 * factory, establish connection to JMS, create a session, resolve the destination by a queue or topic name, create
 * producer, create consumer, set message listener to a consumer, and start connection. This class handles exceptions
 * for all the functionalities provided.
 */
public class JMSConnection {

    private static final Logger LOG = LoggerFactory.getLogger(JMSConnection.class);
    private final JMSConfig config;

    public JMSConnection(JMSConfig config) {
        this.config = config;
    }

    public Context getContext() {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, config.getJndiContextFactory());
        properties.put(Context.PROVIDER_URL, config.getProviderUrl());

        if (!(Strings.isNullOrEmpty(config.getJndiUsername()) && Strings.isNullOrEmpty(config.getJndiPassword()))) {
            properties.put(Context.SECURITY_PRINCIPAL, config.getJndiUsername());
            properties.put(Context.SECURITY_CREDENTIALS, config.getJndiPassword());
        }

        try {
            return new InitialContext(properties);
        } catch (NamingException e) {
            LOG.error("Initial context couldn't get created.", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public ConnectionFactory getConnectionFactory(Context context) {
        try {
            return (ConnectionFactory) context.lookup(config.getConnectionFactory());
        } catch (NamingException e) {
            LOG.error("Connection factory couldn't get resolved.", e);
            throw new RuntimeException(e);
        }
    }

    public Connection createConnection(ConnectionFactory factory) {
        try {
            return factory.createConnection(config.getJmsUsername(), config.getJmsPassword());
        } catch (JMSException e) {
            LOG.error("Connection couldn't get created.", e);
            throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
        }
    }

    public void startConnection(Connection connection) {
        try {
            connection.start();
        } catch (JMSException e) {
            LOG.error("Connection couldn't get started.", e);
            throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
        }
    }

    public void stopConnection(Connection connection){
        try {
            connection.stop();
        } catch (JMSException e) {
            LOG.error("Connection couldn't get stopped!");
            throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
        }
    }

    public void closeConnection(Connection connection) {
        try {
            connection.close();
        } catch (JMSException e) {
            LOG.error("Connection couldn't get closed!");
            throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
        }
    }

    public void closeSession(Session session) {
        try {
            session.close();
        } catch (JMSException e) {
            LOG.error("Session couldn't get closed!");
            throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
        }
    }

    public Session createSession(Connection connection) {
        try {
            return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            LOG.error("Session couldn't get established.", e);
            throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
        }
    }

    public Destination getDestination(Context context) {
        if (config.getType().equals(JMSDestinationType.TOPIC.getName())) {
            try {
                return (Topic) context.lookup(config.getDestination());
            } catch (NamingException e) {
                LOG.error("Cannot resolve the topic with the given name.", e);
                throw new RuntimeException(e.getMessage());
            }
        } else {
            try {
                return (Queue) context.lookup(config.getDestination());
            } catch (NamingException e) {
                LOG.error("Cannot resolve the queue with the given name.", e);
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    public MessageConsumer createConsumer(Session session, Destination destination) {
        try {
            return session.createConsumer(destination);
        } catch (JMSException e) {
            LOG.error("Consumer couldn't get created.", e);
            throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
        }
    }

    public MessageProducer createProducer(Session session, Destination destination) {
        try {
            return session.createProducer(destination);
        } catch (JMSException e) {
            LOG.error("Producer couldn't get created.", e);
            throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
        }
    }

    public void setMessageListener(MessageListener messageListener, MessageConsumer messageConsumer) {
        try {
            messageConsumer.setMessageListener(messageListener);
        } catch (JMSException e) {
            LOG.error("Message listener couldn't get set.", e);
            throw new RuntimeException(String.format("%s: %s", e.getErrorCode(), e.getMessage()));
        }
    }
}
