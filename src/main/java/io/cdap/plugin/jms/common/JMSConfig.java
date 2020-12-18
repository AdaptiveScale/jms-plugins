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

package io.cdap.plugin.jms.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Holds configuration required for configuring {@link io.cdap.plugin.jms.source.JMSSourceUtils;} and
 * {@link io.cdap.plugin.jms.sink.JMSBatchSink}.
 */
public class JMSConfig extends ReferencePluginConfig implements Serializable {
  // Schema
  public static final String MESSAGE_ID = "messageId";
  public static final String MESSAGE_TIMESTAMP = "messageTimestamp";
  public static final String CORRELATION_ID = "correlationId";
  public static final String REPLY_TO = "replyTo";
  public static final String DESTINATION = "destination";
  public static final String DELIVERY_MODE = "deliveryNode";
  public static final String REDELIVERED = "redelivered";
  public static final String TYPE = "type";
  public static final String EXPIRATION = "expiration";
  public static final String PRIORITY = "priority";

  // Params
  public static final String NAME_CONNECTION_FACTORY = "connectionFactory";
  public static final String NAME_JMS_USERNAME = "jmsUsername";
  public static final String NAME_JMS_PASSWORD = "jmsPassword";
  public static final String NAME_PROVIDER_URL = "providerUrl";
  public static final String NAME_TYPE = "type";
  public static final String NAME_QUEUE_OR_TOPIC_NAME = "name";
  public static final String NAME_JNDI_CONTEXT_FACTORY = "jndiContextFactory";
  public static final String NAME_JNDI_USERNAME = "jndiUsername";
  public static final String NAME_JNDI_PASSWORD = "jndiPassword";
  public static final String NAME_MESSAGE_TYPE = "messageType";
  public static final String NAME_SCHEMA = "schema";

  @Name(NAME_CONNECTION_FACTORY)
  @Description("Name of the connection factory.")
  @Nullable
  @Macro
  private String connectionFactory; // default: ConnectionFactory

  @Name(NAME_JMS_USERNAME)
  @Description("Username to connect to JMS.")
  @Macro
  private String jmsUsername;

  @Name(NAME_JMS_PASSWORD)
  @Description("Password to connect to JMS.")
  @Macro
  private String jmsPassword;

  @Name(NAME_PROVIDER_URL)
  @Description("The URL of the JMS provider. For example, in case of an ActiveMQ Provider, the URL has the format " +
    "tcp://hostname:61616.")
  @Macro
  private String providerUrl;

  @Name(NAME_TYPE)
  @Description("Queue or Topic.")
  @Nullable
  @Macro
  private String type; // default: queue

  @Name(NAME_QUEUE_OR_TOPIC_NAME)
  @Description("Queue/Topic name. If the given Queue/Topic name is not resolved, a new Queue/Topic with " +
    "the given name will get created.")
  @Macro
  private String name;

  @Name(NAME_JNDI_CONTEXT_FACTORY)
  @Description("Name of the JNDI context factory. For example, in case of an ActiveMQ Provider, the JNDI Context " +
    "Factory is: org.apache.activemq.jndi.ActiveMQInitialContextFactory.")
  @Macro
  private String jndiContextFactory;

  @Name(NAME_JNDI_USERNAME)
  @Description("User name for the JNDI.")
  @Nullable
  @Macro
  private String jndiUsername;

  @Name(NAME_JNDI_PASSWORD)
  @Description("Password for the JNDI.")
  @Nullable
  @Macro
  private String jndiPassword;

  @Name(NAME_MESSAGE_TYPE)
  @Description("Supports the following message types: Message, Text, Bytes, Map.")
  @Nullable
  @Macro
  private String messageType; // default: Text

  @Name(NAME_SCHEMA)
  @Nullable
  @Description("Specifies the schema of the records outputted from this plugin.")
  private String schema;

  public JMSConfig() {
    super("");
    this.connectionFactory = "ConnectionFactory";
    this.type = JMSDestinationType.QUEUE.getName();
    this.messageType = JMSMessageType.TEXT.getName();
  }

  @VisibleForTesting
  public JMSConfig(String referenceName, String connectionFactory, String jmsUsername, String jmsPassword,
                   String providerUrl, String type, String jndiContextFactory, String jndiUsername,
                   String jndiPassword, String messageType) {
    super(referenceName);
    this.connectionFactory = Strings.isNullOrEmpty(connectionFactory) ? "ConnectionFactory" : connectionFactory;
    this.jmsUsername = jmsUsername;
    this.jmsPassword = jmsPassword;
    this.providerUrl = providerUrl;
    this.type = Strings.isNullOrEmpty(type) ? JMSDestinationType.QUEUE.getName() : type;
    this.jndiContextFactory = jndiContextFactory;
    this.jndiUsername = jndiUsername;
    this.jndiPassword = jndiPassword;
    this.messageType = Strings.isNullOrEmpty(messageType) ? JMSMessageType.TEXT.getName() : messageType;
    this.name = name;
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

  public String getName() {
    return name;
  }

  public void validate(FailureCollector failureCollector) {

    if (Strings.isNullOrEmpty(jmsUsername) && !containsMacro(NAME_JMS_USERNAME)) {
      failureCollector
        .addFailure("JMS username must be provided.", "Please provide your JMS username.")
        .withConfigProperty(NAME_JMS_USERNAME);
    }

    if (Strings.isNullOrEmpty(jmsPassword) && !containsMacro(NAME_JMS_PASSWORD)) {
      failureCollector
        .addFailure("JMS password must be provided.", "Please provide your JMS password.")
        .withConfigProperty(NAME_JMS_PASSWORD);
    }

    if (Strings.isNullOrEmpty(jndiContextFactory) && !containsMacro(NAME_JNDI_CONTEXT_FACTORY)) {
      failureCollector
        .addFailure("JNDI context factory must be provided.", "Please provide your JNDI" +
          " context factory.")
        .withConfigProperty(NAME_JNDI_CONTEXT_FACTORY);
    }

    if (Strings.isNullOrEmpty(providerUrl) && !containsMacro(NAME_PROVIDER_URL)) {
      failureCollector
        .addFailure("Provider URL must be provided.", "Please provide your provider URL.")
        .withConfigProperty(NAME_PROVIDER_URL);
    }

    if (Strings.isNullOrEmpty(name) && !containsMacro(NAME_QUEUE_OR_TOPIC_NAME)) {
      failureCollector
        .addFailure("Destination must be provided.", "Please provide your topic/queue name.")
        .withConfigProperty(NAME_QUEUE_OR_TOPIC_NAME);
    }
  }

  public Schema getSpecificSchema(String type) {
    List<Schema.Field> baseSchemaFields = new ArrayList<Schema.Field>(
      Arrays.asList(
        Schema.Field.of(MESSAGE_ID, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(MESSAGE_TIMESTAMP, Schema.of(Schema.Type.LONG)),
        Schema.Field.of(CORRELATION_ID, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(REPLY_TO, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(DESTINATION, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(DELIVERY_MODE, Schema.of(Schema.Type.INT)),
        Schema.Field.of(REDELIVERED, Schema.of(Schema.Type.BOOLEAN)),
        Schema.Field.of(TYPE, Schema.of(Schema.Type.STRING)),
        Schema.Field.of(EXPIRATION, Schema.of(Schema.Type.LONG)),
        Schema.Field.of(PRIORITY, Schema.of(Schema.Type.INT))
      ));

    if (type.equals(JMSMessageType.MESSAGE.getName())) {
      return Schema.recordOf("message", baseSchemaFields);
    } else if (type.equals(JMSMessageType.BYTES.getName())) {
      baseSchemaFields.add(Schema.Field.of("payload", Schema.arrayOf(Schema.of(Schema.Type.BYTES))));
      return Schema.recordOf("message", baseSchemaFields);
    } else if (type.equals(JMSMessageType.MAP.getName())) {
      baseSchemaFields.add(Schema.Field.of("payload", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                   Schema.of(Schema.Type.STRING))));
      return Schema.recordOf("message", baseSchemaFields);
    } else {
      // Text
      baseSchemaFields.add(Schema.Field.of("payload", Schema.of(Schema.Type.STRING)));
      return Schema.recordOf("message", baseSchemaFields);
    }
  }
}
