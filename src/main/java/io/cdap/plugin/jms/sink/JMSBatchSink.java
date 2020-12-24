package io.cdap.plugin.jms.sink;

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

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.jms.common.JMSConfig;
import io.cdap.plugin.jms.common.JMSConnection;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("JMS")
@Description("JMS sink to write events to JMS")
public class JMSBatchSink extends ReferenceBatchSink<StructuredRecord, Text, Text> {

  private static final String ASYNC = "async";
  private static final String TOPIC = "topic";

  private final JMSConfig config;
  private final JMSOutputFormatProvider jmsOutputFormatProvider;
  private Connection connection;
  private Session session;
  private MessageProducer messageProducer;
  private final JMSConnection jmsConnection;

  private static final Logger logger = LoggerFactory.getLogger(JMSBatchSink.class);

  public JMSBatchSink(JMSConfig config) {
    super(config);
    this.config = config;
    this.jmsOutputFormatProvider = new JMSOutputFormatProvider(config);
    this.jmsConnection = new JMSConnection(config);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSpecificSchema(config.getMessageType()));
  }

  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) throws Exception {
    LineageRecorder lineageRecorder = new LineageRecorder(batchSinkContext, config.referenceName);
    Schema schema = batchSinkContext.getInputSchema();
    Map<String, String> arguments = new HashMap<>();
    if (schema != null) {
      lineageRecorder.createExternalDataset(schema);
      if (schema.getFields() != null && !schema.getFields().isEmpty()) {
        lineageRecorder.recordWrite("Write", "Wrote to JMS topic.", schema.getFields().stream().map
                (Schema.Field::getName).collect(Collectors.toList()));
      }
    }

    batchSinkContext.addOutput(Output.ofDataset(config.referenceName, arguments));
//    context.addOutput(Output.of(config.referenceName, new SinkOutputFormatProvider()));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Text, Text>> emitter) throws Exception {
    Context context = jmsConnection.getContext();

    ConnectionFactory factory = jmsConnection.getConnectionFactory(context);

    connection = jmsConnection.createConnection(factory);

    session = jmsConnection.createSession(connection);

    Destination destination = jmsConnection.getDestination(context);

    messageProducer = jmsConnection.createProducer(session, destination);

    TextMessage textMessage = session.createTextMessage();
    textMessage.setText("Hello hi hi ...");

    messageProducer.send(textMessage);
    connection.close();
    emitter.emit(new KeyValue<>(new Text(""), new Text(textMessage.getText())));
  }

  @Override
  public void destroy() {
    super.destroy();
  }

  private static class JMSOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    JMSOutputFormatProvider(Config config) {
      this.conf = new HashMap<>();
//      conf.put(TOPIC, config.)
    }

    @Override
    public String getOutputFormatClassName() {
      return null;
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return null;
    }
  }
}
