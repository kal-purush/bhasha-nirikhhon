using System;
using System.Collections.Generic;
using System.Messaging;

namespace Shared
{
    public class MessageHandler
    {

        public void sendMessage(string queueName, string message)
        {
            MessageQueue messageQueue = null;
            string description = "Bookstore Order";

            try
            {
                if (MessageQueue.Exists(queueName))
                {
                    messageQueue = new MessageQueue(queueName);
                    messageQueue.Label = description;
                }
                else
                {
                    MessageQueue.Create(queueName);
                    messageQueue = new MessageQueue(queueName);
                    messageQueue.Label = description;
                }
                
                messageQueue.Send(message);
            }
            catch{ throw; }
            finally
            {
                messageQueue.Dispose();
            }
        }

        public void sendOrder(string queueName, Order order)
        {
            MessageQueue messageQueue = null;
            if (!MessageQueue.Exists(queueName))
                messageQueue = MessageQueue.Create(queueName);
            else
                messageQueue = new MessageQueue(queueName);

            try
            {
                messageQueue.Formatter = new XmlMessageFormatter(new Type[] { typeof(Order) });
                messageQueue.Send(order);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.InnerException);
            }
            finally
            {
                messageQueue.Close();
            }
        }

        public List<string> ReadQueue(string queueName)
        {
            List<string> lstMessages = new List<string>();
            using (MessageQueue messageQueue = new MessageQueue(queueName))
            {
                System.Messaging.Message[] messages = messageQueue.GetAllMessages();

                foreach(System.Messaging.Message message in messages)
                {
                    message.Formatter = new XmlMessageFormatter(
                        new String[] { "System.String, mscorlib" }
                    );

                    string msg = message.Body.ToString();
                    lstMessages.Add(msg);

                }
            }
            return lstMessages;
        }

        public Order ReceiveOrder(string queueName)
        {
            if (!MessageQueue.Exists(queueName))
                return null;

            MessageQueue messageQueue = new MessageQueue(queueName);
            Order order = null;

            try
            {
                messageQueue.Formatter = new XmlMessageFormatter(new Type[] { typeof(Order) });
                order = (Order)messageQueue.Receive().Body;
            }
            catch { }
            finally
            {
                messageQueue.Close();
            }
            return order;
        }
    }
}