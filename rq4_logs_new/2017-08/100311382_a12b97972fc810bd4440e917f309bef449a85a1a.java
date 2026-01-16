package com.websocket.java.config.db;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.websocket.java.util.PropertiesReader;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * Created by anton on
 */
public class MongoDbConfiguration {

    private static final CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider.builder().automatic(true).build()));
    private static final MongoClientURI connectionString = new MongoClientURI(PropertiesReader.get("db.ip") + ":" + PropertiesReader.get("db.port"));
    private static final MongoClient mongoClient = new MongoClient(connectionString);
    private static final MongoDatabase mongoDatabase = mongoClient.getDatabase(PropertiesReader.get("db.name")).withCodecRegistry(pojoCodecRegistry);


    public MongoDatabase getConnection() {
        return mongoDatabase;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

}