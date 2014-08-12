/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.risbic.intraconnect.jms;

import javax.annotation.Resource;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlowNode;
import com.arjuna.databroker.data.DataProvider;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class JMSDataProvider<T extends Serializable> implements DataProvider<T>
{
   public static final String DATA_FLOW_NODE_ID = "DATA_FLOW_NODE_ID";

   public static final String DATA_PROVIDER_ID = "DATA_PROVIDER_ID";

   private static final Logger logger = Logger.getLogger(JMSDataProvider.class.getName());

   @Resource(mappedName = "java:/JmsXA")
   private static ConnectionFactory cf;

   @Resource(lookup = "jms/risbic/data/provider/interconnect")
   private static Topic topic;

   private DataFlowNode dfNode;

   private List<DataConsumer<T>> consumers;

   private MessageProducer producer;

   private Session session;

   private String dataProviderId;

   /**
    * Creates a new JMSDataProvider.  This data provider should be backed by a Durable Topic to which consumers will
    * be dynamically subscribed.  Message filters are used to send the correct messages to the Data Consumers.  The
    * filter is currently based on the dfNode name.
    *
    * @param dataFlowNode The DataFlowNode that generates the data for this JMS Data Provider.
    */
   public JMSDataProvider(DataFlowNode dataFlowNode)
   {
      try
      {
         logger.log(Level.FINE, "JMSDataProvider: " + dataFlowNode);

         dfNode = dataFlowNode;
         consumers = new ArrayList<DataConsumer<T>>();

         Connection connection = cf.createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         producer = session.createProducer(topic);
         connection.start();
      }
      catch(Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   @Override
   public DataFlowNode getDataFlowNode()
   {
      return dfNode;
   }

   @Override
   public Collection<DataConsumer<T>> getDataConsumers()
   {
      return Collections.unmodifiableList(consumers);
   }

   @Override
   public void addDataConsumer(DataConsumer<T> dataConsumer)
   {
      if(dataConsumer instanceof JMSDataConsumer)
      {
         consumers.add(dataConsumer);
         ((JMSDataConsumer) dataConsumer).setProvider(this);
      }
      else
      {
         throw new RuntimeException("Type: " + dataConsumer.getClass().getName() +
                                                                    " is not supported by JMSDataProivder");
      }
   }

   @Override
   public void removeDataConsumer(DataConsumer<T> dataConsumer)
   {
      //unsubscribe consumer from topic
      consumers.remove(dataConsumer);
   }

   @Override
   public void produce(T data)
   {
      try
      {
         Message message = session.createObjectMessage(data);
         message.setStringProperty(DATA_FLOW_NODE_ID, dfNode.getName());
         message.setStringProperty(DATA_FLOW_NODE_ID, dfNode.getName());
         producer.send(message);
      }
      catch(Exception e)
      {
         throw new RuntimeException(e);
      }
   }
}
