# JMS Streaming Source


Description
-----------
Consumes JMS messages of different types as Message, Text, Bytes and Map from a specified Queue or Topic.

Use Case
--------
Use this JMS Source plugin when you want to consume messages from a JMS Queue/Topic and write them to a Table. 


Properties
----------
**Connection Factory**: Name of the connection factory. If not specified *ConnectionFactory* if considered by default.

**JMS Username**: Username to connect to JMS. This property is mandatory.

**JMS Password**: Password to connect to JMS. This property is mandatory.

**Provider URL**: Provider URL of the JMS Provider. This property is mandatory. For example for the *ActiveMQ* provider
 you can set: `tcp://hostname:61616`.

**Type**: Queue or Topic. Queue is considered by default.

**JNDI Context Factory**: Name of the context factory. This property is optional. For example for the *ActiveMQ* 
provider you can set: `org.apache.activemq.jndi.ActiveMQInitialContextFactory`.

**JNDI Username**: Username for JNDI. This property is optional.

**JNDI Password**: Password for JNDI. This property is optional.

**Message Type**: The type of messages you intend to consume. A JMS message could be of the following types: *Message*,
 *Text*, *Bytes* and *Map*. By default, *Text* message type is considered. The *payload* field of the output schema gets
  switched to the appropriate data type upon the selection of a message type and *validate* button click. 


Example
-------
This example reads JMS messages of *TextMessage* type from the *status* topic existing in provider *tcp://hostname
:616161* with JNDI context factory name *org.apache.activemq.jndi.ActiveMQInitialContextFactory*. An example of a
TextMessage object is shown below. Since we used ActiveMQ as a provider, the message is`1 automatically considered as an
*ActiveMQTextMessage* (an implementation of JMS TextMessage interface). The object below shows an
*ActiveMQTextMessage* consumed: 

```text
ActiveMQTextMessage{
  commandId=5,
  responseRequired=true,
  messageId=ID: Producer-50444-1608735228752-1: 1: 1: 1: 1,
  originalDestination=null,
  originalTransactionId=null,
  producerId=ID: Producer-50444-1608735228752-1: 1: 1: 1,
  destination=topic: topic://status,
  transactionId=null,
  expiration=0,
  timestamp=1608735228894,
  arrival=0,
  brokerInTime=1608735228895,
  brokerOutTime=1608735228896,
  correlationId=null,
  replyTo=null,
  persistent=true,
  type=null,
  priority=4,
  groupID=null,
  groupSequence=0,
  targetConsumerId=null,
  compressed=false,
  userID=null,
  content=org.apache.activemq.util.ByteSequence@4b9e255,
  marshalledProperties=null,
  dataStructure=null,
  redeliveryCounter=0,
  size=0,
  properties=null,
  readOnlyProperties=true,
  readOnlyBody=true,
  droppable=false,
  jmsXGroupFirstForConsumer=false,
  text=DONE
}

```
Since the JMS Source plugin's implementation is purely based in JMS (ie. not coupled in any JMS implementation as for
example ActiveMQ), we consider only the header data supported by JMS. The consumed will output the below
record: 

```
| field name        | data                                       |
| ----------------- | ------------------------------------------ |
| deliveryNode      | 2                                          |
| payload           | DONE                                       |
| replyTo          | topic://status                             |
| messageId        | ID:Producer-54511-1608749039578-1:1:1:1:1  |
| correlationId    | null                                       |
| expiration        | 0                                          |
| type              | null                                       |
| redelivered       | false                                      |
```
