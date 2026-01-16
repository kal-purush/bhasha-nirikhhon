package groupone.userservice.config;

//@Configuration
public class RabbitConfig {
////
////    @Value("${spring.rabbitmq.host}")
////    private String host;
////
////    @Value("${spring.rabbitmq.username}")
////    private String username;
////
////    @Value("${spring.rabbitmq.password}")
////    private String password;
//
//    private final CachingConnectionFactory cachingConnectionFactory;
//
//    public RabbitConfig(CachingConnectionFactory cachingConnectionFactory) {
//        this.cachingConnectionFactory = cachingConnectionFactory;
//    }
//
////    @Bean
////    public Queue createUserRegistrationQueue() {
////        return QueueBuilder.durable("q.user-registration")
////                .withArgument("x-dead-letter-exchange", "x.registration-failure")
////                .withArgument("x-dead-letter-routing-key", "fall-back")
////                .build();
////    }
//
//    @Bean
//    public StatefulRetryOperationsInterceptor retryInterceptor() {
//        StatefulRetryOperationsInterceptor interceptor =
//                RetryInterceptorBuilder.stateful()
//                        .maxAttempts(5)
//                        .backOffOptions(1, 2, 10) // initialInterval, multiplier, maxInterval
//                        .build();
////        return RetryInterceptorBuilder.stateless().maxAttempts(3)
////                .backOffOptions(2000, 2.0, 100000)
//////                .recoverer(new ImmediateRequeueMessageRecoverer())
//////                .recoverer(new RejectAndDontRequeueRecoverer())
////                .build();
//        return interceptor;
//    }
//
//    @Bean
//    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(SimpleRabbitListenerContainerFactoryConfigurer configurer) {
//        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
//        configurer.configure(factory, cachingConnectionFactory);
//        factory.setAcknowledgeMode(AcknowledgeMode.AUTO);
//        factory.setAdviceChain(retryInterceptor());
//        factory.setDefaultRequeueRejected(false);
//        return factory;
//    }
//
////    @Bean
////    public Declarables createPostRegistrationSchema() {
////        return new Declarables(
////                new DirectExchange("x.post-registration"),
////                new Queue("q.send-email"),
////                new Binding("q.send-email", Binding.DestinationType.QUEUE, "x.post-registration", "send-email", null)
////        );
////    }
////
////    @Bean
////    public Declarables createDeadLetterSchema() {
////        return new Declarables(
////                new DirectExchange("x.registration-failure"),
////                new Queue("q.fall-back-registration"),
////                new Binding("q.fall-back-registration", Binding.DestinationType.QUEUE, "x.registration-failure", "fall-back", null)
////        );
////    }
//
//    @Bean
//    public Jackson2JsonMessageConverter converter() {
//        return new Jackson2JsonMessageConverter();
//    }
//
//    @Bean
//    public RabbitTemplate rabbitTemplate(Jackson2JsonMessageConverter converter) {
//        RabbitTemplate template = new RabbitTemplate(cachingConnectionFactory);
//        template.setMessageConverter(converter);
//        return template;
//    }
}