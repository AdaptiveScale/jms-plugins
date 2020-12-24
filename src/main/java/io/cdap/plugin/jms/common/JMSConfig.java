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

public class JMSConfig extends ReferencePluginConfig implements Serializable {
  @Name("ConnectionFactory")
  @Description("Name of the connection factory")
  @Nullable
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
  @Nullable
  @Macro
  private String jndiContextFactory;

  @Name("JNDIUsername")
  @Description("User name for JNDI")
  @Nullable
  @Macro
  private String jndiUsername;

  @Name("JNDIPassword")
  @Description("password for JNDI")
  @Nullable
  @Macro
  private String jndiPassword;

  @Name("MessageType")
  @Description("Message, Text, Bytes, Object, Map")
  @Macro
  private String messageType;

  @Name("schema")
  @Nullable
  @Description("Specifies the schema of the records outputted from this plugin.")
  private String schema;

  @Name("Destination")
  @Description("Destination (Queue/Topic) name.")
  private String destination;

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

  public JMSConfig()
  {
    super("");
    this.connectionFactory = "ConnectionFactory";
    this.type = "Queue";
    this.messageType = "Text";
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
    this.type = Strings.isNullOrEmpty(type) ? "Queue" : type;
    this.jndiContextFactory = jndiContextFactory;
    this.jndiUsername = jndiUsername;
    this.jndiPassword = jndiPassword;
    this.messageType = Strings.isNullOrEmpty(messageType) ? "Text" : messageType;
    this.destination = destination;
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

  public String getDestination() { return destination; }

  public void validate(FailureCollector failureCollector) {

    if (Strings.isNullOrEmpty(this.jmsUsername)) {
      failureCollector.addFailure("JMS username must be provided", null)
        .withConfigProperty(jmsUsername);
    }

    if (Strings.isNullOrEmpty(this.jmsPassword)) {
      failureCollector.addFailure("JMS password must be provided", null)
        .withConfigProperty(jmsPassword);
    }

    if (Strings.isNullOrEmpty(this.providerUrl)) {
      failureCollector.addFailure("Provider URL must be provided", null)
        .withConfigProperty(jmsPassword);
    }

    if (Strings.isNullOrEmpty(this.type)) {
      failureCollector.addFailure("Type must be provided", null);
    }

    if (Strings.isNullOrEmpty(this.type)) {
      failureCollector.addFailure("Message type must be provided", null);
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

    switch (type) {
      case "Message":
        return Schema.recordOf("message", baseSchemaFields);

      case "Bytes":
        baseSchemaFields.add(Schema.Field.of("payload", Schema.arrayOf(Schema.of(Schema.Type.BYTES))));
        return Schema.recordOf("message", baseSchemaFields);

      case "Map":
        baseSchemaFields.add(Schema.Field.of("payload", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                     Schema.of(Schema.Type.STRING))));
        return Schema.recordOf("message", baseSchemaFields);

      case "Object":
        baseSchemaFields.add(Schema.Field.of("payload", Schema.of(Schema.Type.STRING)));
        return Schema.recordOf("message", baseSchemaFields);

      default:
        baseSchemaFields.add(Schema.Field.of("payload", Schema.of(Schema.Type.STRING)));
        return Schema.recordOf("message", baseSchemaFields);
    }
  }
}
