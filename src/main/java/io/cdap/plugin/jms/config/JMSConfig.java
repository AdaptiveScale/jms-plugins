package io.cdap.plugin.jms.config;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.io.Serializable;

public class JMSConfig extends ReferencePluginConfig implements Serializable {
  @Name("ConnectionFactory")
  @Description("Name of the connection factory")
  @Macro
  private String connectionFactory;

  @Name("JMSUsername")
  @Description("Username to connect to JMS")
  @Macro
  private String jmsUsername;

  @Name("JMSPassword")
  @Description("Password to connect to JMS")
  @Macro
  private String jmsPassword;

  @Name("ProviderURL")
  @Description("Provide URL for JMS provider")
  @Macro
  private String providerUrl;

  @Name("Type")
  @Description("Queue or Topic")
  @Macro
  private String type;

  @Name("JNDIContextFactory")
  @Description("Name of the contact factory")
  @Macro
  private String jndiContextFactory;

  @Name("JNDIUsername")
  @Description("User name for JNDI")
  @Macro
  private String jndiUsername;

  @Name("JNDIPassword")
  @Description("password for JNDI")
  @Macro
  private String jndiPassword;

  @Name("MessageType")
  @Description("Message, Text, Bytes, Object, Map")
  @Macro
  private String messageType;

  public JMSConfig(String referenceName, String connectionFactory, String jmsUsername, String jmsPassword,
                            String providerUrl, String type, String jndiContextFactory, String jndiUsername,
                            String jndiPassword, String messageType) {
    super(referenceName);
    this.connectionFactory = Strings.isNullOrEmpty(connectionFactory) ? "ConnectionFactory" : connectionFactory;
    this.jmsUsername = jmsUsername;
    this.jmsPassword = jmsPassword;
    this.providerUrl = providerUrl;
    this.type = Strings.isNullOrEmpty(type) ? "Queue" : type;
    this.jndiContextFactory = jndiContextFactory;
    this.jndiUsername = jndiUsername;
    this.jndiPassword = jndiPassword;
    this.messageType = Strings.isNullOrEmpty(messageType) ? "Text" : messageType;
  }

  public String getConnectionFactory() {
    return connectionFactory;
  }

  public String getJmsUsername() {
    return jmsUsername;
  }

  public String getJmsPassword() {
    return jmsPassword;
  }

  public String getProviderUrl() {
    return providerUrl;
  }

  public String getType() {
    return type;
  }

  public String getJndiContextFactory() {
    return jndiContextFactory;
  }

  public String getJndiUsername() {
    return jndiUsername;
  }

  public String getJndiPassword() {
    return jndiPassword;
  }

  public String getMessageType() {
    return messageType;
  }

  private void validate(FailureCollector failureCollector) {

    if (Strings.isNullOrEmpty(this.jmsUsername)) {
      failureCollector.addFailure("JMS username must be provided", null);
    }

    if (Strings.isNullOrEmpty(this.jmsPassword)) {
      failureCollector.addFailure("JMS password must be provided", null);
    }

    if (Strings.isNullOrEmpty(this.providerUrl)) {
      failureCollector.addFailure("Provider URL must be provided", null);
    }

    if (Strings.isNullOrEmpty(this.type)) {
      failureCollector.addFailure("Type must be provided", null);
    }

    if (Strings.isNullOrEmpty(this.type)) {
      failureCollector.addFailure("Message type must be provided", null);
    }
  }
}
