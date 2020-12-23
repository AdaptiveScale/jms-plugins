package io.cdap.plugin.jms.source;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.jms.config.JMSConfig;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JMSReceiver extends Receiver<StructuredRecord> implements MessageListener {

  private static Logger logger = LoggerFactory.getLogger(JMSReceiver.class);
  private JMSConfig config;
  private Connection connection;
  private StorageLevel storageLevel;
  private Session session;

  public JMSReceiver(StorageLevel storageLevel, JMSConfig config) {
    super(storageLevel);
    this.storageLevel = storageLevel;
    this.config = config;
  }

  @Override
  public void onStart() {

    Context context = getContext(config);

    ConnectionFactory factory = getConnectionFactory(context);

    connection = createConnection(factory);

    session = createSession(connection);

    Destination queue = getDestination(context);

    MessageConsumer messageConsumer = createConsumer(queue);

    setMessageListener(this, messageConsumer);
    startConnection(connection);
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
    } catch (Exception e) {
      logger.error("Message couldn't be stored in spark memory.", e);
      throw new RuntimeException(e);
    }
  }

  private Context getContext(JMSConfig config) {
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

  private ConnectionFactory getConnectionFactory(Context context) {
    try {
      return (ConnectionFactory) context.lookup(config.getConnectionFactory());
    } catch (NamingException e) {
      logger.error("Exception when trying to do JNDI API lookup failed.", e);
      throw new RuntimeException(e);
    }
  }

  private Connection createConnection(ConnectionFactory factory) {
    try {
      return factory.createConnection(config.getJmsUsername(), config.getJmsPassword());
    } catch (JMSException e) {
      logger.error("Connection couldn't be created.", e);
      throw new RuntimeException(e);
    }
  }

  private Session createSession(Connection connection) {
    try {
      return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    } catch (JMSException e) {
      logger.error("Session couldn't be created.", e);
      throw new RuntimeException(e);
    }
  }

  private Destination getDestination(Context context) {
    try {
      return (Destination) context.lookup("MyTopic");
    } catch (NamingException e) {
      logger.error("Exception when trying to do queue lookup failed.", e);
      throw new RuntimeException(e);
    }
  }

  private MessageConsumer createConsumer(Destination queue) {
    try {
      return session.createConsumer(queue);
    } catch (JMSException e) {
      logger.error("Consumer couldn't be created.", e);
      throw new RuntimeException(e);
    }
  }

  private void setMessageListener(MessageListener messageListener, MessageConsumer messageConsumer) {
    try {
      messageConsumer.setMessageListener(messageListener);
    } catch (JMSException e) {
      logger.error("Message listener couldn't be set.", e);
      throw new RuntimeException(e);
    }
  }

  private void startConnection(Connection connection) {
    try {
      connection.start();
    } catch (JMSException e) {
      logger.error("Connection couldn't be started.", e);
      throw new RuntimeException(e);
    }
  }
}
