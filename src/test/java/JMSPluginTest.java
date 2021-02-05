///*
// * Copyright © 2021 Cask Data, Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
//
//import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
//import org.junit.Assert;
//import org.junit.Test;
//import org.mockito.internal.util.reflection.FieldSetter;
//
///**
// * Unit tests for the plugin
// */
//public class JMSPluginTest {
//  private JMSConfig getValidConfig(String fileSystemProperties) throws NoSuchFieldException {
//    JMSConfig jmsConfig = new JMSConfig();
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("connectionFactory"), "ConFactory");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("jmsUsername"), "jms_username");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("jmsPassword"), "jms_password");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("providerUrl"), "tcp://0.0.0.0:61616");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("type"), "Queue");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("destination"), "test-topic");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("jndiContextFactory"), "ContextFactory");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("jndiUsername"), "jndi-username");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("jndiPassword"), "jndi-password");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("messageType"), "Text");
//    return jmsConfig;
//  }
//
//  private JMSConfig getInvalidConfig(String fileSystemProperties) throws NoSuchFieldException {
//    JMSConfig jmsConfig = new JMSConfig();
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("connectionFactory"), "ConFactory");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("jmsUsername"), "jms_username");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("jmsPassword"), "jms_password");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("providerUrl"), "tcp://0.0.0.0:61616");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("type"), "Queue");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("destination"), null);
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("jndiContextFactory"), "ContextFactory");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("jndiUsername"), "jndi-username");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("jndiPassword"), "jndi-password");
//    FieldSetter.setField(jmsConfig, JMSConfig.class.getDeclaredField("messageType"), "Text");
//    return jmsConfig;
//  }
//
//  @Test
//  public void testValidFSProperties() throws NoSuchFieldException {
//    JMSConfig jmsConfig = getValidConfig("{\"key\":\"val\"}");
//    MockFailureCollector collector = new MockFailureCollector("jms_failure_collector");
//    jmsConfig.validate(collector);
//    Assert.assertEquals(0, collector.getValidationFailures().size());
//  }
//
//  @Test
//  public void testInvalidFSProperties() throws NoSuchFieldException {
//    JMSConfig jmsConfig = getInvalidConfig("{\"key\":\"val\"}");
//    MockFailureCollector collector = new MockFailureCollector("jms_failure_collector");
//    jmsConfig.validate(collector);
//    Assert.assertEquals(1, collector.getValidationFailures().size());
//  }
//}
