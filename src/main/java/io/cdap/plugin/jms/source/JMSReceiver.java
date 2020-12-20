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

public class JMSReceiver extends Receiver<StructuredRecord> implements MessageListener {

  private JMSConfig config;
  private Connection connection;
  private StorageLevel storageLevel;
  private static Logger logger = LoggerFactory.getLogger(JMSReceiver.class);


  public JMSReceiver(StorageLevel storageLevel, JMSConfig config) {
    super(storageLevel);
    this.storageLevel = storageLevel;
    this.config = config;
  }

  public void onStart() {

    try {
      Properties properties = new Properties();
      properties.put(Context.INITIAL_CONTEXT_FACTORY, config.getJndiContextFactory());
      properties.put(Context.PROVIDER_URL, config.getProviderUrl());
      logger.info("PARA KONTEKSTIT");
      Context context = new InitialContext(properties);
      logger.info("PAS KONTEKSTIT");
      logger.info(config.getJndiContextFactory());
      logger.info(config.getProviderUrl());

      ConnectionFactory factory = (ConnectionFactory) context.lookup(config.getConnectionFactory());
      connection = factory.createConnection(config.getJmsUsername(), config.getJmsPassword());
      logger.info("LUJTA BABOO");
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Destination queue = (Destination) context.lookup("MyQueue");

      MessageConsumer messageConsumer = session.createConsumer(queue);

      messageConsumer.setMessageListener(this);
      connection.start();
    }
    catch (Exception ex) {
      System.out.println("drd");
    }
  }

  public void onStop() {
    try {
      connection.stop();
    }
    catch (JMSException exception) {
      System.out.println(exception);
    }
  }

  public void onMessage(Message message) {
    try {
      store( JMSSourceUtils.convertMessage(message));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
