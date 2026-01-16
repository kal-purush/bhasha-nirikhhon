package pl.btcgrouppl.btc.backend.commons.test;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.jms.Jms;
import org.springframework.integration.dsl.support.Transformers;
import org.springframework.integration.json.JsonToObjectTransformer;
import org.springframework.integration.support.json.Jackson2JsonObjectMapper;
import org.springframework.jms.core.JmsTemplate;
import pl.btcgrouppl.btc.backend.commons.Constants;

import java.util.concurrent.Executors;

/**
 * Created by Sebastian Mekal <sebitg@gmail.com> on 03.07.15.
 * <p>
 *     JMS profile test spring configuration
 * </p>
 */
@Configuration
@Profile(Constants.PROFILES.PROFILE_INTEGRATION_JMS)
@EnableAutoConfiguration
@EnableIntegration
@ComponentScan
@IntegrationComponentScan
public class BtcBackendCommonsJmsTestSpringConfiguration {


    @Bean(name = TestConstants.INTEGRATION.TEST_CHANNEL_DIRECT)
    @Qualifier(TestConstants.INTEGRATION.TEST_CHANNEL_DIRECT)
    public DirectChannel testDirectChannel() {
        return MessageChannels.direct(TestConstants.INTEGRATION.TEST_CHANNEL_DIRECT).get();
    }

    @Bean(name = TestConstants.INTEGRATION.TEST_CHANNEL_PUBSUB)
    @Qualifier(TestConstants.INTEGRATION.TEST_CHANNEL_PUBSUB)
    public PublishSubscribeChannel testPubSubChannel() {
        return MessageChannels.publishSubscribe(TestConstants.INTEGRATION.TEST_CHANNEL_PUBSUB, Executors.newCachedThreadPool()).get();
    }

    @Bean(name = TestConstants.INTEGRATION.TEST_FLOW_OUT)
    @Qualifier(TestConstants.INTEGRATION.TEST_FLOW_OUT)
    public IntegrationFlow testFlowOut(@Qualifier(TestConstants.INTEGRATION.TEST_CHANNEL_DIRECT) DirectChannel testDirectChannel, JmsTemplate jmsTemplate,
                                       Jackson2JsonObjectMapper jackson2JsonObjectMapper) {
        return IntegrationFlows.from(testDirectChannel)
                .transform(Transformers.toJson(jackson2JsonObjectMapper))
                .handle(Jms.outboundAdapter(jmsTemplate).destination(TestConstants.INTEGRATION.TEST_DESTINATION))
                .get();
    }

    @Bean(name = TestConstants.INTEGRATION.TEST_FLOW_IN)
    @Qualifier(TestConstants.INTEGRATION.TEST_FLOW_IN)
    public IntegrationFlow testFlowIn(@Qualifier(TestConstants.INTEGRATION.TEST_CHANNEL_PUBSUB) PublishSubscribeChannel testPubSubChannel, JmsTemplate jmsTemplate,
                                      JsonToObjectTransformer integrationMessageJsonToObjectTransformer) {
        return IntegrationFlows.from(Jms.inboundAdapter(jmsTemplate).destination(TestConstants.INTEGRATION.TEST_DESTINATION))
                .transform(integrationMessageJsonToObjectTransformer)
                .channel(testPubSubChannel)
                .get();
    }
}