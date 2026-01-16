package kr.ac.kumoh.illdang100.tovalley.config.kafka;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import kr.ac.kumoh.illdang100.tovalley.kafka.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@EnableKafka // Spring Kafka 활성화 -> Kafka Listener 사용 가능
@Configuration
public class ListenerConfiguration {

    @Value("${kafka.server}")
    private String kafkaServer;

    @Value("${kafka.consumer.id}")
    private String kafkaConsumerGroupId;

    // KafkaListener 컨테이너 팩토리를 생성하는 Bean 메서드
    /*
    ConcurrentKafkaListenerContainerFactory : KafkaListener 어노테이션이 달린 메서드 처리
    이를 위한 ConsumerFactory와 ContainerProperties를 설정한다.
    여기서 ConsumerFactory는 Kafka Consumer를 생성하고, ContainerProperties는 리스너 컨테이너의 구성을 정의한다.
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Message> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Message> factory = new ConcurrentKafkaListenerContainerFactory<>();

        /*
        ConsumerFactory : ConsumerFactory 인터페이스는 Kafka Consumer 인스턴스를 생성하는 데 사용된다.
        여기서는 DefaultKafkaConsumerFactory를 사용하며,
        이 클래스는 Kafka Consumer를 생성하는 데 필요한 설정들을 받아서 Kafka Consumer를 생성한다.
         */
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    // Kafka ConsumerFactory를 생성하는 Bean 메서드
    @Bean
    public ConsumerFactory<String, Message> consumerFactory() {
        /*
        JsonDeserializer : Kafka에서 메시지를 받을 때 JSON 형태의 메시지를 자바 객체로 역직렬화하는 데 사용된다.
        여기서는 Message 클래스의 인스턴스로 역직렬화하도록 설정되어 있다.
         */
        JsonDeserializer<Message> deserializer = new JsonDeserializer<>();
        // 패키지 신뢰 오류로 인해 모든 패키지를 신뢰하도록 작성
        deserializer.addTrustedPackages("*");

        // Kafka Consumer 구성을 위한 설정값들을 설정 -> 변하지 않는 값이므로 ImmutableMap을 이용하여 설정
        /*
        ConsumerConfig :Kafka Consumer의 설정값들을 정의하는 상수들을 가지고 있다.
        여기서는 Kafka 서버의 위치(BOOTSTRAP_SERVERS_CONFIG),
        Consumer 그룹 ID(GROUP_ID_CONFIG),
        키의 역직렬화 클래스(KEY_DESERIALIZER_CLASS_CONFIG),
        값의 역직렬화 클래스(VALUE_DESERIALIZER_CLASS_CONFIG),
        그리고 오프셋 리셋 설정(AUTO_OFFSET_RESET_CONFIG) 등을 설정하고 있다.
         */
        Map<String, Object> consumerConfigurations =
                ImmutableMap.<String, Object>builder()
                        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer)
                        .put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerGroupId)
                        .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                        .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
                        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                        .build();

        return new DefaultKafkaConsumerFactory<>(consumerConfigurations, new StringDeserializer(), deserializer);
    }
}