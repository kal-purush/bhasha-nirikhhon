/**
 *  Copyright 2005-2018 Red Hat, Inc.
 *
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.mycompany;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

/**
 * A spring-boot application that includes a Camel route builder to setup the Camel routes
 */
@SpringBootApplication
@ImportResource({"classpath:spring/camel-context.xml"})
public class Application {

    // must have a main method spring-boot can run
    public static void main(String[] args) throws Exception {
        try {
            
            CamelContext ctx = createCamelContext();
            ctx.start();
            ctx.addRoutes(new RouteBuilder() {
                @Override
                public void configure() throws Exception {
                    /* Our direct route will take a message, and set the message to group 1 if the body is an integer,
                     * otherwise set the group to 2.
                     *
                     * This demonstrates the following concepts:
                     *  1) Header Manipulation
                     *  2) Checking the payload type of the body and using it in a choice.
                     *  3) JMS Message groups
                     */

                   from("direct:begin")
                    .choice()
                        .when(body().isInstanceOf(Integer.class)).setHeader("JMSXGroupID",constant("1"))
                        .otherwise().setHeader("JMSXGroupID",constant("2"))
                    .end()
                    //.setHeader("AMQPriority", constant(""))
                    .to("amq:queue:Message.Group.Test");
                   
                   
                    /* These two are competing consumers */
                   from("amq:queue:Message.Group.Test").routeId("Route A").log("Received: ${body}");
                   from("amq:queue:Message.Group.Test").routeId("Route B").log("Received: ${body}");
                }
            });

            sendMessages(ctx.createProducerTemplate());
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
           // stopBroker();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static CamelContext createCamelContext() throws Exception {
            CamelContext camelContext = new DefaultCamelContext();

           // ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("vm://localhost/");
            
            /*JmsConnectionFactory jmsConnectionFactory = 
            		new JmsConnectionFactory("tcp://localhost:61616");*/
            
            /*camelContext.addComponent("amqp",
                    JmsComponent.jmsComponentAutoAcknowledge(jmsConnectionFactory));*/
            
            
            PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory("tcp://localhost:61616");
            pooledConnectionFactory.setMaxConnections(8);
            pooledConnectionFactory.setMaximumActiveSessionPerConnection(500);

            ActiveMQComponent activeMQComponent = ActiveMQComponent.activeMQComponent();
            activeMQComponent.setUsePooledConnection(true);
            activeMQComponent.setConnectionFactory(pooledConnectionFactory);
            camelContext.addComponent("amq", activeMQComponent);

            return camelContext;
        }
        
        private static void sendMessages(ProducerTemplate pt) throws Exception {
            for (int i = 0; i < 10; i++) {
                pt.sendBody("direct:begin", Integer.valueOf(i));
            }

            for (int i = 0; i < 10; i++) {
                pt.sendBody("direct:begin", "next group");
            }

            pt.sendBody("direct:begin", Integer.valueOf(1));
            pt.sendBody("direct:begin", "foo");
            pt.sendBody("direct:begin", Integer.valueOf(2));
        }
}