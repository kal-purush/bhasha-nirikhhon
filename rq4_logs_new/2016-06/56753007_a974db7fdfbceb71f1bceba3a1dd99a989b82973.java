/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


package se.project.ecampusconnect.jms;

/**
 *
 * @author Advait
 */

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JMSConsumer 
{
    public static void main(String[] args) {
      String destName = null;
      Context jndiContext = null;
      ConnectionFactory connectionFactory = null;
      Connection connection = null;
      Session session = null;
      Destination dest = null;
      MessageConsumer consumer = null;
      TextMessage message = null;

      if (args.length != 1) {
          System.out.println("Program takes one argument: <dest_name>");
          System.exit(1);
      }

      destName = new String(args[0]);
      System.out.println("Destination name is " + destName);

      /*
       * Create a JNDI API InitialContext object if none exists
       * yet.
       */
      try {
          jndiContext = new InitialContext();
      } catch (NamingException e) {
          System.out.println("Could not create JNDI API context: " +
              e.toString());
          System.exit(1);
      }

      /*
       * Look up connection factory and destination.  If either
       * does not exist, exit.  If you look up a
       * TopicConnectionFactory or a QueueConnectionFactory,
       * program behavior is the same.
       */
      try {
          connectionFactory = (ConnectionFactory) jndiContext.lookup(
                  "jms/ConnectionFactory");
          dest = (Destination) jndiContext.lookup(destName);
      } catch (Exception e) {
          System.out.println("JNDI API lookup failed: " + e.toString());
          System.exit(1);
      }

      /*
       * Create connection.
       * Create session from connection; false means session is
       * not transacted.
       * Create consumer, then start message delivery.
       * Receive all text messages from destination until
       * a non-text message is received indicating end of
       * message stream.
       * Close connection.
       */
      try {
          connection = connectionFactory.createConnection();
          session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
          consumer = session.createConsumer(dest);
          connection.start();

          while (true) {
              Message m = consumer.receive(1);

              if (m != null) {
                  if (m instanceof TextMessage) {
                      message = (TextMessage) m;
                      System.out.println("Reading message: " +
                          message.getText());
                  } else {
                      break;
                  }
              }
          }
      } catch (JMSException e) {
          System.out.println("Exception occurred: " + e.toString());
      } finally {
          if (connection != null) {
              try {
                  connection.close();
              } catch (JMSException e) {
              }
          }
      }
  }
    
}