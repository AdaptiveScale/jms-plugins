# JMS batch sink


Description
-----------
Produces JMS messages of different types as Message, Text, Bytes and Map to a specified Queue or Topic.

Use Case
--------
Use this JMS Sink plugin when you want to produce messages to a JMS Queue/Topic. 


Properties
----------
**Connection Factory**: Name of the connection factory. If not specified, the value *ConnectionFactory* is considered by
 default.

**JMS Username**: Username to connect to JMS. This property is mandatory.

**JMS Password**: Password to connect to JMS. This property is mandatory.

**Provider URL**: Provider URL of the JMS Provider. This property is mandatory. For example for the *ActiveMQ* provider
 you can set: `tcp://hostname:61616`.

**Type**: Queue or Topic. Queue is considered by default.

**Destination**: Queue/Topic name.

**JNDI Context Factory**: Name of the context factory. This property is optional. For example for the *ActiveMQ* 
provider you can set: `org.apache.activemq.jndi.ActiveMQInitialContextFactory`.

**JNDI Username**: Username for JNDI. This property is optional.

**JNDI Password**: Password for JNDI. This property is optional.

**Message Type**: The type of messages you intend to produce. A JMS message could be of the following types: *Message*,
 *Text*, *Bytes* and *Map*. By default, *Text* message type is considered.

Example
-------
This example reads a record from a JSON file and produces a *MapMessage* with the file's content as a payload.
For every record of the JSON file it produces one *MapMessage* to the given topic.
An example of a JSON record is shown below.

```json
{"name":"foo", "surname":"bar", "age":23}
```
We could see the produced message in the running JMS implementation.

| field name        | value                                      |
| ----------------- | ------------------------------------------ |
| messageId         | ID:Producer-36705-1609122138230-1:1:1:1:1  |
| messageTimestamp  | 1609122138554                              |
| destination       | topic://MyTopic                            |
| deliveryNode      | 2                                          |
| expiration        | 0                                          |
| priority          | 4                                          |
| payload           | {"name":"foo","surname":"bar","age":23}    |
