package com.lym.business.simple.config;

import com.rabbitmq.client.ConnectionFactory;

//@Configuration
//@Component
public class RabbitMqProcuderConfig {

    //工厂
   // @Bean
    public static ConnectionFactory connectionFactory(){
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.96.120");
        //factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        //是否支持自动重连
        //factory.setAutomaticRecoveryEnabled(true);
        //3000秒执行一次
        //factory.setNetworkRecoveryInterval(3000);
        factory.setVirtualHost("/");
        return factory;
    }

}