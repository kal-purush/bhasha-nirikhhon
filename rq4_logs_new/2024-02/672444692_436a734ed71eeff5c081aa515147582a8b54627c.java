package kr.ac.kumoh.illdang100.tovalley.config.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@Configuration
@RequiredArgsConstructor
@EnableMongoRepositories(basePackages = "kr.ac.kumoh.illdang100.tovalley.domain.chat")
// 스프링 데이터 몽고DB가 kr.ac.kumoh.illdang100.tovalley.domain.chat 패키지 안의 Repository 인터페이스를 스캔하도록 설정한다.
public class MongoConfig {

    private final MongoProperties mongoProperties;  // MongoDB 연결과 관련된 프로퍼티를 주입받습니다.

    @Bean
    public MongoClient mongoClient() {
        // MongoDB에 연결하는 MongoClient 객체를 생성합니다. MongoProperties 객체에 정의된 설정을 사용하여 클라이언트를 생성합니다.
        return MongoClients.create(mongoProperties.getClient());
    }

    // 동적 쿼리 문제를 해결하려면 MongoTemplate의 도움을 받아야 한다.
    @Bean
    public MongoTemplate mongoTemplate() {
        // MongoTemplate 객체를 생성한다. 이 객체는 MongoDB 데이터베이스와의 CRUD 작업을 수행하는데 사용된다.
        // MongoClient 객체와 MongoProperties에 정의된 데이터베이스 이름을 사용하여 MongoTemplate 객체를 생성한다.
        return new MongoTemplate(mongoClient(), mongoProperties.getName());
    }
}