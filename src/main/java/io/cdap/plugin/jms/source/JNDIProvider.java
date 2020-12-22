package io.cdap.plugin.jms.source;

import io.cdap.plugin.jms.config.JMSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JNDIProvider {
  private static final Logger LOG = LoggerFactory.getLogger(JNDIProvider.class);

  private final ConnectionFactory connectionFactory;
  private final Destination destination;

  public JNDIProvider(JMSConfig config) {

    Properties initialContextProps = new Properties();
    initialContextProps.put(Context.INITIAL_CONTEXT_FACTORY, config.getJndiContextFactory());
    initialContextProps.put(Context.PROVIDER_URL, config.getProviderUrl());

//    config.getJndiUsername(); Todo: add this at props (required)
//    config.getJndiPassword();

//    config.getJmsUsername(); Todo: add this at props (optional)
//    config.getJmsPassword();

    Context jndiContext = null;
    try {
      jndiContext = new InitialContext(initialContextProps);
    } catch (NamingException e) {
      LOG.error("Exception when creating initial context for connection factory.", e);
      throw new RuntimeException(e);
    }

    try {
      connectionFactory = (ConnectionFactory) jndiContext.lookup(config.getConnectionFactory());
      destination = (Destination) jndiContext.lookup("MyTopic"); // Todo: fix this
    } catch (NamingException e) {
      LOG.error("Exception when trying to do JNDI API lookup failed.", e);
      throw new RuntimeException(e);
    }
  }

  public ConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }

  public Destination getDestination() {
    return destination;
  }
}
