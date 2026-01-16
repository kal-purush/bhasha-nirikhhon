package real_spring.never_use_switch;

import com.github.javafaker.Faker;
import homework.lab3.RandomUtils;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class DataProducer {
    private final Faker faker = new Faker();

    public  Message generateMessage() {
        return Message.builder()
                .messageChannel(RandomUtils.getRandomElement(Arrays.asList(MessageChannel.values())))
                .content(faker.witcher().quote())
                .build();
    }

}