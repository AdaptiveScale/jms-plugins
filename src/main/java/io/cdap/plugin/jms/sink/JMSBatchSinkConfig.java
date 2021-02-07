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

package io.cdap.plugin.jms.sink;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.jms.common.JMSConfig;

/**
 * Holds configuration required for configuring {@link io.cdap.plugin.jms.source.JMSSourceUtils;} and
 * {@link JMSBatchSink}.
 */
public class JMSBatchSinkConfig extends JMSConfig {

  // Params
  public static final String NAME_DESTINATION = "destinationName";

  @Name(NAME_DESTINATION)
  @Description("Name of the destination Queue/Topic. If the given Queue/Topic name is not resolved, a new " +
    "Queue/Topic with the given name will get created.")
  @Macro
  private String destinationName;

  public JMSBatchSinkConfig() {
    super();
  }

//  public JMSBatchSinkConfig(String referenceName, String connectionFactory, String jmsUsername,
//                                  String jmsPassword, String providerUrl, String type, String jndiContextFactory,
//                                  String jndiUsername, String jndiPassword, String messageType) {
//    super(referenceName, connectionFactory, jmsUsername, jmsPassword, providerUrl, type, jndiContextFactory,
//          jndiUsername, jndiPassword, messageType);
//    this.destinationName = destinationName;
//  }

  public String getDestinationName() {
    return destinationName;
  }

  public void validate(FailureCollector failureCollector) {
    if (Strings.isNullOrEmpty(destinationName) && !containsMacro(NAME_DESTINATION)) {
      failureCollector
        .addFailure("Destination must be provided.", "Please provide your topic/queue name.")
        .withConfigProperty(NAME_DESTINATION);
    }
  }
}
