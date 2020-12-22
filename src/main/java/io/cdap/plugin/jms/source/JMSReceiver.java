package io.cdap.plugin.jms.source;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.jms.config.JMSConfig;
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

public class JMSReceiver extends Receiver<StructuredRecord> implements MessageListener {

  private static Logger logger = LoggerFactory.getLogger(JMSReceiver.class);
  private JMSConfig config;
  private Connection connection;
  private StorageLevel storageLevel;
  private static final Logger LOG = LoggerFactory.getLogger(JMSReceiver.class);


  public JMSReceiver(StorageLevel storageLevel, JMSConfig config) {
    super(storageLevel);
    this.storageLevel = storageLevel;
    this.config = config;
  }

  public void onStart() {
    JNDIProvider jndiProvider = new JNDIProvider(this.config);
    ConnectionFactory factory = jndiProvider.getConnectionFactory();

    try {
      connection = factory.createConnection(config.getJmsUsername(), config.getJmsPassword());
    } catch (JMSException e) {
      LOG.error("Connection couldn't be created.", e);
      throw new RuntimeException(e);
    }

    Session session = null;
    try {
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    } catch (JMSException e) {
      LOG.error("Session couldn't be created.", e);
      throw new RuntimeException(e);
    }

    Destination destination = jndiProvider.getDestination();
    MessageConsumer messageConsumer = null;
    try {
      messageConsumer = session.createConsumer(destination);
    } catch (JMSException e) {
      LOG.error("Consumer couldn't be created.", e);
      throw new RuntimeException(e);
    }

    try {
      connection.start();
    } catch (JMSException e) {
      LOG.error("Connection couldn't be started.", e);
      throw new RuntimeException(e);
    }
  }

  public void onStop() {
    try {
      connection.stop();
    } catch (JMSException e) {
      LOG.error("Connection couldn't be stopped.", e);
      throw new RuntimeException(e);
    }
  }

  public void onMessage(Message message) {
    try {
      store(JMSSourceUtils.convertMessage(message, this.config));
    } catch (Exception e) {
      LOG.error("Message couldn't be stored in spark memory.", e);
      throw new RuntimeException(e);
    }
  }
}
