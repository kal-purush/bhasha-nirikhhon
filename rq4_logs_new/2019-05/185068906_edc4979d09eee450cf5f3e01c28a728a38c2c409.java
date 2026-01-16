package com.quikr.assessment.ESearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.quikr.assessment.ESearch.model.Comment;

@SpringBootApplication
public class Main {

	public static void main(String[] args) {
		SpringApplication.run(Main.class, args);
	}

	static final String queueName = "commentQueue";

	/* uncomment this method for consumer*/
//	@Bean
//	public MessageConverter messageConverter() {
//		return new Jackson2JsonMessageConverter();
//	}

	@Bean
	public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setMessageConverter(messageConverter());
		return template;
	}
	
	/*comment this method for consumer*/
    @Bean
    public MessageConverter messageConverter()
    {
        Jackson2JsonMessageConverter jsonMessageConverter = new Jackson2JsonMessageConverter();
        jsonMessageConverter.setClassMapper(classMapper());
        return jsonMessageConverter;
    }

    @Bean
    public DefaultClassMapper classMapper()
    {
        DefaultClassMapper classMapper = new DefaultClassMapper();
        Map<String, Class<?>> idClassMapping = new HashMap<>();
        idClassMapping.put("com.quikr.assessment.ESearch.model.Comment", Comment.class);
        classMapper.setIdClassMapping(idClassMapping);
        return classMapper;
    }

//	@Bean
//	public Exchange eventExchange() {
//		return new TopicExchange("eventExchange");
//	}
//
//	@Bean
//	public Queue queue() {
//		return new Queue(queueName);
//	}
//
//	@Bean
//	public Binding binding(Queue queue, Exchange eventExchange) {
//		return BindingBuilder.bind(queue).to(eventExchange).with("comment.*").noargs();
//	}

	@Value("${elasticsearch.host:localhost}")
	public String host;
	@Value("${elasticsearch.port:9300}")
	public int port;

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	@Bean
	public Client client() throws UnknownHostException {
		TransportClient client = null;
		try {
			System.out.println("host:" + host + "port:" + port);
			client = new PreBuiltTransportClient(Settings.EMPTY)
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return client;

	}
}