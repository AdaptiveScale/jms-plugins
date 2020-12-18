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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.ReferenceBatchSink;
import io.cdap.plugin.jms.config.JMSConfig;
import org.apache.hadoop.io.Text;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("JMS")
@Description("JMS sink to write events to JMS")
public class JMSBatchSink extends ReferenceBatchSink<StructuredRecord, Text, Text> {
  private final JMSConfig config;

  public JMSBatchSink(JMSConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<Text, Text>> emitter) throws Exception {

  }

  @Override
  public void destroy() {
    super.destroy();
  }
}
