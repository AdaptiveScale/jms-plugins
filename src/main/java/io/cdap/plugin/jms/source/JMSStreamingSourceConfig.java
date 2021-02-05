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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Holds configuration required for configuring {@link JMSSourceUtils;} and
 * {@link io.cdap.plugin.jms.sink.JMSBatchSink}.
 */
public class JMSStreamingSourceConfig extends JMSConfig {
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
  public static final String NAME_SKIP_MESSAGE_HEADERS = "skipMessageHeaders";

  @Name(NAME_SOURCE)
  @Description("Name of the source Queue/Topic. The Queue/Topic with the given name, should exist in order to read " +
    "messages from.")
  @Macro
  private String sourceName;

  @Name(NAME_SKIP_MESSAGE_HEADERS)
  @Description("If true, only the JMS message payload is considered. Otherwise, also message headers are considered.")
  @Macro
  private String skipMessageHeaders;

  @Name(NAME_SCHEMA)
  @Nullable
  @Description("Specifies the schema of the records outputted from this plugin.")
  private String schema;

  public JMSStreamingSourceConfig() {
    super();
  }

  @VisibleForTesting
  public JMSStreamingSourceConfig(String referenceName, String connectionFactory, String jmsUsername,
                                  String jmsPassword, String providerUrl, String type, String jndiContextFactory,
                                  String jndiUsername, String jndiPassword, String skipMessageHeaders,
                                  String messageType) {
    super(referenceName, connectionFactory, jmsUsername, jmsPassword, providerUrl, type, jndiContextFactory,
          jndiUsername, jndiPassword, messageType);
    this.sourceName = sourceName;
    this.skipMessageHeaders = skipMessageHeaders;
  }

  @Override
  public void validate(FailureCollector failureCollector) {

    if (Strings.isNullOrEmpty(sourceName) && !containsMacro(NAME_SOURCE)) {
      failureCollector
        .addFailure("Destination must be provided.", "Please provide your topic/queue name.")
        .withConfigProperty(NAME_SOURCE);
    }
  }
  public boolean getSkipMessageHeaders() {
    return this.skipMessageHeaders.equalsIgnoreCase("true");
  }

  public String getSourceName() {
    return sourceName;
  }


  public Schema getSpecificSchema(String type, boolean skipMessageHeaders) {
    List<Schema.Field> baseSchemaFields = new ArrayList<>();

    if (skipMessageHeaders) {
      baseSchemaFields = Arrays.asList(
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
      );
    }

//    if (type.equals(JMSMessageType.MESSAGE.getName())) {
//      return Schema.recordOf("message", baseSchemaFields); // todo: handle message
//    } else

    if (type.equals(JMSMessageType.BYTES.getName())) {
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
