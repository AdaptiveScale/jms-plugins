package io.cdap.plugin.jms.common;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Properties;

public class JMSConnection {

    private static Logger logger = LoggerFactory.getLogger(JMSConnection.class);
    private final JMSConfig config;

    public JMSConnection(JMSConfig config) {
        this.config = config;
    }


    public Context getContext() {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, config.getJndiContextFactory());
        properties.put(Context.PROVIDER_URL, config.getProviderUrl());

        try {
            return new InitialContext(properties);
        } catch (NamingException e) {
            logger.error("Exception when creating initial context for connection factory.", e);
            throw new RuntimeException(e);
        }
    }

    public ConnectionFactory getConnectionFactory(Context context) {
        try {
            return (ConnectionFactory) context.lookup(config.getConnectionFactory());
        } catch (NamingException e) {
            logger.error("Exception when trying to do JNDI API lookup failed.", e);
            throw new RuntimeException(e);
        }
    }

    public Connection createConnection(ConnectionFactory factory) {
        try {
            return factory.createConnection(config.getJmsUsername(), config.getJmsPassword());
        } catch (JMSException e) {
            logger.error("Connection couldn't be created.", e);
            throw new RuntimeException(e);
        }
    }

    public Session createSession(Connection connection) {
        try {
            return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            logger.error("Session couldn't be created.", e);
            throw new RuntimeException(e);
        }
    }

    public Destination getDestination(Context context) {
        try {
            return (Destination) context.lookup("MyTopic");
        } catch (NamingException e) {
            logger.error("Exception when trying to do queue lookup failed.", e);
            throw new RuntimeException(e);
        }
    }

    public MessageConsumer createConsumer(Session session, Destination destination) {
        try {
            return session.createConsumer(destination);
        } catch (JMSException e) {
            logger.error("Consumer couldn't be created.", e);
            throw new RuntimeException(e);
        }
    }

    public MessageProducer createProducer(Session session, Destination destination) {
        try {
            return session.createProducer(destination);
        } catch (JMSException e) {
            logger.error("Producer couldn't be created.", e);
            throw new RuntimeException(e);
        }
    }

    public void setMessageListener(MessageListener messageListener, MessageConsumer messageConsumer) {
        try {
            messageConsumer.setMessageListener(messageListener);
        } catch (JMSException e) {
            logger.error("Message listener couldn't be set.", e);
            throw new RuntimeException(e);
        }
    }

    public void startConnection(Connection connection) {
        try {
            connection.start();
        } catch (JMSException e) {
            logger.error("Connection couldn't be started.", e);
            throw new RuntimeException(e);
        }
    }
}
