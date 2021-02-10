package io.cdap.plugin.jms.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.jms.common.JMSConfig;
import io.cdap.plugin.jms.common.JMSMessageType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Configs for {@link JMSStreamingSource}
 */
public class JMSStreamingSourceConfig extends JMSConfig implements Serializable {
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
  public static final String NAME_SOURCE = "sourceName";
  public static final String NAME_SCHEMA = "schema";
  public static final String NAME_REMOVE_MESSAGE_HEADERS = "removeMessageHeaders";
  public static final String NAME_MESSAGE_TYPE = "messageType";


  @Name(NAME_SOURCE)
  @Description("Name of the source Queue/Topic. The Queue/Topic with the given name, should exist in order to read " +
    "messages from.")
  @Macro
  private String sourceName;

  @Name(NAME_REMOVE_MESSAGE_HEADERS)
  @Description("If true, only the JMS message payload is considered. Otherwise, message headers are also considered.")
  @Macro
  private String removeMessageHeaders;

  @Name(NAME_SCHEMA)
  @Nullable
  @Description("Specifies the schema of the records outputted from this plugin.")
  private String schema;

  @Name(NAME_MESSAGE_TYPE)
  @Description("Supports the following message types: Message, Text, Bytes, Map, Object.")
  @Nullable
  @Macro
  private String messageType; // default: Text

  public JMSStreamingSourceConfig() {
    super("");
    this.removeMessageHeaders = Strings.isNullOrEmpty(removeMessageHeaders) ? "false" : removeMessageHeaders;
    this.messageType = Strings.isNullOrEmpty(messageType) ? JMSMessageType.TEXT : messageType;
  }

  @VisibleForTesting
  public JMSStreamingSourceConfig(String referenceName, String connectionFactory, String jmsUsername,
                                  String jmsPassword, String providerUrl, String type, String jndiContextFactory,
                                  String jndiUsername, String jndiPassword, String removeMessageHeaders,
                                  String messageType, String sourceName) {
    super(referenceName, connectionFactory, jmsUsername, jmsPassword, providerUrl, type, jndiContextFactory,
          jndiUsername, jndiPassword);
    this.sourceName = sourceName;
    this.removeMessageHeaders = removeMessageHeaders;
    this.messageType = messageType;
  }

  public void validateParams(FailureCollector failureCollector) {
    validate(failureCollector);

    if (Strings.isNullOrEmpty(sourceName) && !containsMacro(NAME_SOURCE)) {
      failureCollector
        .addFailure("The source topic/queue name must be provided!", "Provide your topic/queue name.")
        .withConfigProperty(NAME_SOURCE);
    }
    if (Strings.isNullOrEmpty(messageType) && !containsMacro(NAME_SOURCE)) {
      failureCollector
        .addFailure("The source topic/queue name must be provided!", "Provide your topic/queue name.")
        .withConfigProperty(NAME_SOURCE);
    }
  }

  public boolean getRemoveMessageHeaders() {
    return this.removeMessageHeaders.equalsIgnoreCase("true");
  }

  public String getSourceName() {
    return sourceName;
  }

  @Nullable
  public String getMessageType() {
    return messageType;
  }

  public Schema getSpecificSchema(String type, boolean removeMessageHeaders) {
    List<Schema.Field> baseSchemaFields = new ArrayList<>();

    if (!removeMessageHeaders) {
      baseSchemaFields.addAll(Arrays.asList(
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
    }

    switch (type) {
      case JMSMessageType.MESSAGE:
        baseSchemaFields.add(Schema.Field.of("payload", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                     Schema.of(Schema.Type.STRING))));
        return Schema.recordOf("message", baseSchemaFields);
      case JMSMessageType.BYTES:
        baseSchemaFields.add(Schema.Field.of("payload", Schema.arrayOf(Schema.of(Schema.Type.BYTES))));
        return Schema.recordOf("message", baseSchemaFields);
      case JMSMessageType.MAP:
        baseSchemaFields.add(Schema.Field.of("payload", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                                     Schema.of(Schema.Type.STRING))));
        return Schema.recordOf("message", baseSchemaFields);
      case JMSMessageType.OBJECT:
        baseSchemaFields.add(Schema.Field.of("payload", Schema.arrayOf(Schema.of(Schema.Type.BYTES))));
        return Schema.recordOf("message", baseSchemaFields);
      default:
        baseSchemaFields.add(Schema.Field.of("payload", Schema.of(Schema.Type.STRING)));
        return Schema.recordOf("message", baseSchemaFields);
    }
  }
}
