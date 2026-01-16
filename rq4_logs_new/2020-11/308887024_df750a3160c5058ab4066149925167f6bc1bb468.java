package com.marketplace;

import com.marketplace.config.ApplicationConfig;
import com.marketplace.config.ConfigLoader;
import com.marketplace.context.ApplicationContext;
import com.marketplace.context.DaggerApplicationContext;
import com.marketplace.context.mongo.MongoConfig;
import com.marketplace.context.mongo.MongoConfigModule;
import com.marketplace.domain.classifiedad.ClassifiedAd;
import com.marketplace.domain.classifiedad.repository.ClassifiedAdRepository;
import com.marketplace.domain.classifiedad.service.ClassifiedAdService;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

public abstract class BaseMongoRepositoryTest extends AbstractContainerInitializer {
    MongoConfig mongoConfig;
    MongoClient mongoClient;
    MongoCollection<ClassifiedAd> classifiedAdCollection;
    ApplicationContext context;
    ClassifiedAdRepository classifiedAdRepository;
    ClassifiedAdService classifiedAdService;

    @BeforeEach
    void setup() throws IOException {
        ApplicationConfig config = ConfigLoader.loadClasspathResource("application.yml", ApplicationConfig.class);

        context = DaggerApplicationContext.
                builder()
                .config(config)
                .build();

        String hosts = mongoDBContainer.getHost();
        int port = mongoDBContainer.getMappedPort(27017);
        mongoConfig = new MongoConfig(hosts, "test_db", port);
        config.setMongoConfig(mongoConfig);
        mongoClient = MongoConfigModule.provideMongoClient(mongoConfig);
        classifiedAdCollection = MongoConfigModule.provideMongoDatabase(mongoClient, mongoConfig)
                .getCollection(ClassifiedAd.class.getSimpleName().toLowerCase(), ClassifiedAd.class);
        classifiedAdRepository = context.getClassifiedAdRepository();
        classifiedAdService = context.getClassifiedAdService();
    }

    @AfterEach
    public void cleanup() {
        DeleteResult deleteResult = classifiedAdCollection.deleteMany(new Document());
        if (deleteResult.wasAcknowledged()) {
            System.out.println("delete was acknowledged.");
        }
        System.out.println("Number of records deleted: " + deleteResult.getDeletedCount());
    }

}