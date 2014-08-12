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
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlowNode;
import com.arjuna.databroker.data.DataProvider;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class JMSDataConsumer<T extends Serializable> implements DataConsumer<T>
{
   private static final Logger logger = Logger.getLogger(JMSDataConsumer.class.getName());

   @Resource(mappedName = "java:/JmsXA")
   private static ConnectionFactory cf;

   @Resource(lookup = "jms/risbic/data/provider/interconnect")
   private static Topic topic;

   private DataFlowNode dfNode;

   private Session session;

   private MessageConsumer consumer;

   private String methodName;

   private Class<T> dataClass;

   public JMSDataConsumer(DataFlowNode dfNode, String methodName, Class<T> dataClass)
   {
      try
      {
         this.dfNode = dfNode;
         this.methodName = methodName;
         this.dataClass = dataClass;

         Connection connection = cf.createConnection();
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         consumer = session.createConsumer(topic);
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
   public void consume(DataProvider<T> dataProvider, T data)
   {
      try
      {
         getMethod(dfNode.getClass(),methodName).invoke(dfNode, data);
      }
      catch (Throwable throwable)
      {
         logger.log(Level.WARNING, "Problem invoking consumer", throwable);
      }
   }

   private Method getMethod(Class<?> nodeClass, String nodeMethodName)
   {
      try
      {
         return nodeClass.getMethod(nodeMethodName, new Class[]{dataClass});
      }
      catch (Throwable throwable)
      {
         logger.log(Level.WARNING, "Unable to find method \"" + nodeMethodName + "\"", throwable);

         return null;
      }

   @Override
   public void consume(DataProvider<T> dataProvider, T t)
   {
      // We don't need this methods we consume messages as they appear on the topic.
   }

   public void setProvider(JMSDataProvider<T> provider)
   {
      providers.add(provider);
   }

   // TODO We need some life cycle methods for these objects.  This unsubscribe method should be called "onDestroy".
   public void unsubscribe()
   {

   }

   private class MessageListener implements javax.jms.MessageListener
   {

      private MessageListener() {};}

      @Override
      public void onMessage(Message message)
      {
         try
         {
            if (message instanceof ObjectMessage)
            {
               Object o = ((ObjectMessage) message).getObject();
               getMethod(dfNode.getClass(), methodName).invoke(dfNode, ((ObjectMessage) message).getObject());
            }
         }
         catch(Exception e)
         {
            throw new RuntimeException(e);
         }
      }
   }
}
