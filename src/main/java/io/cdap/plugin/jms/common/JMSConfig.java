package io.cdap.plugin.jms.common;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.common.ReferencePluginConfig;

import javax.annotation.Nullable;

/**
 * Base config for JMS plugins. todo: fix this
 */
public class JMSConfig extends ReferencePluginConfig {

  public static final String NAME_CONNECTION_FACTORY = "connectionFactory";
  public static final String NAME_JMS_USERNAME = "jmsUsername";
  public static final String NAME_JMS_PASSWORD = "jmsPassword";
  public static final String NAME_PROVIDER_URL = "providerUrl";
  public static final String NAME_TYPE = "type";
  public static final String NAME_JNDI_CONTEXT_FACTORY = "jndiContextFactory";
  public static final String NAME_JNDI_USERNAME = "jndiUsername";
  public static final String NAME_JNDI_PASSWORD = "jndiPassword";
  public static final String NAME_MESSAGE_TYPE = "messageType";

  @Name(NAME_CONNECTION_FACTORY)
  @Description("Name of the connection factory.")
  @Nullable
  @Macro
  public String connectionFactory; // default: ConnectionFactory

  @Name(NAME_JMS_USERNAME)
  @Description("Username to connect to JMS.")
  @Macro
  public String jmsUsername;

  @Name(NAME_JMS_PASSWORD)
  @Description("Password to connect to JMS.")
  @Macro
  public String jmsPassword;

  @Name(NAME_PROVIDER_URL)
  @Description("The URL of the JMS provider. For example, in case of an ActiveMQ Provider, the URL has the format " +
    "tcp://hostname:61616.")
  @Macro
  public String providerUrl;

  @Name(NAME_TYPE)
  @Description("Queue or Topic.")
  @Nullable
  @Macro
  public String type; // default: queue

  @Name(NAME_JNDI_CONTEXT_FACTORY)
  @Description("Name of the JNDI context factory. For example, in case of an ActiveMQ Provider, the JNDI Context " +
    "Factory is: org.apache.activemq.jndi.ActiveMQInitialContextFactory.")
  @Macro
  public String jndiContextFactory; // default: org.apache.activemq.jndi.ActiveMQInitialContextFactory

  @Name(NAME_JNDI_USERNAME)
  @Description("User name for the JNDI.")
  @Nullable
  @Macro
  public String jndiUsername;

  @Name(NAME_JNDI_PASSWORD)
  @Description("Password for the JNDI.")
  @Nullable
  @Macro
  public String jndiPassword;

  @Name(NAME_MESSAGE_TYPE)
  @Description("Supports the following message types: Message, Text, Bytes, Map.")
  @Nullable
  @Macro
  public String messageType; // default: Text


  public JMSConfig() {
    super("");
    this.connectionFactory = "ConnectionFactory";
    this.type = JMSDataStructure.QUEUE.getName();
    this.messageType = JMSMessageType.TEXT.getName();
  }

//  @VisibleForTesting
//  public JMSConfig(String referenceName, String connectionFactory, String jmsUsername, String jmsPassword,
//                   String providerUrl, String type, String jndiContextFactory, String jndiUsername,
//                   String jndiPassword,
//                   String messageType) {
//    super(referenceName);
//    this.connectionFactory = Strings.isNullOrEmpty(connectionFactory) ? "ConnectionFactory" : connectionFactory;
//    this.jmsUsername = jmsUsername;
//    this.jmsPassword = jmsPassword;
//    this.providerUrl = providerUrl;
//    this.type = Strings.isNullOrEmpty(type) ? JMSDataStructure.QUEUE.getName() : type;
//    this.jndiContextFactory = Strings.isNullOrEmpty(jndiContextFactory) ?
//      "org.apache.activemq.jndi.ActiveMQInitialContextFactory" : jndiContextFactory;
//    this.jndiUsername = jndiUsername;
//    this.jndiPassword = jndiPassword;
//    this.messageType = Strings.isNullOrEmpty(messageType) ? JMSMessageType.TEXT.getName() : messageType;
//  }

//  public String getConnectionFactory() {
//    return connectionFactory;
//  }
//
//  public String getJmsUsername() {
//    return jmsUsername;
//  }
//
//  public String getJmsPassword() {
//    return jmsPassword;
//  }
//
//  public String getProviderUrl() {
//    return providerUrl;
//  }
//
//  public String getType() {
//    return type;
//  }
//
//  public String getJndiContextFactory() {
//    return jndiContextFactory;
//  }
//
//  public String getJndiUsername() {
//    return jndiUsername;
//  }
//
//  public String getJndiPassword() {
//    return jndiPassword;
//  }
//
//  public String getMessageType() {
//    return messageType;
//  }
}
