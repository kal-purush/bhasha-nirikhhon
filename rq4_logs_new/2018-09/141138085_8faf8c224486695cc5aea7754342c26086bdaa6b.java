package cms.sre.persistenceapi;

import cms.sre.mongo_connection_helper.MongoClientFactory;
import cms.sre.mongo_connection_helper.MongoClientParameters;
import com.mongodb.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@Profile("!test")
@Configuration
@EnableMongoRepositories
@PropertySource(value="file:///data/persistence-api/configuration/application.properties", ignoreResourceNotFound = true)
public class MongoConfig extends AbstractMongoConfiguration {

    private static Logger logger = LoggerFactory.getLogger(App.class);

    private static String nullWrapper(String value){
        return value == null ? "(null)" : value;
    }

    private static String nullWrapper(String[] values){
        String ret = null;
        if(values != null && values.length > 0){
            ret = "[";
            for (int i = 0, len = values.length; i < len; i++) {
                String value = values[i];

                ret += nullWrapper(value);
                ret += i < len - 1 ? "," : "";
            }
            ret += "]";
        }
        return ret;
    }

    @Value("${mongodb.databaseName:#{null}}")
    private String mongoDatabaseName;

    @Value("${mongodb.keyStoreKeyPassword:#{null}}")
    private String mongoKeyStoreKeyPassword;

    @Value("${mongodb.keyStoreLocation:#{null}}")
    private String mongoKeyStoreLocation;

    @Value("${mongodb.keyStorePassword:#{null}}")
    private String mongoKeyStorePassword;

    @Value("${mongodb.trustStoreLocation:#{null}}")
    private String mongoTrustStoreLocation;

    @Value("${mongodb.trustStorePassword:#{null}}")
    private String mongoTrustStorePassword;

    @Value("${mongodb.username:#{null}}")
    private String mongoUsername;

    @Value("${mongodb.password:#{null}}")
    private String mongoPassword;

    @Value("${mongodb.replicaSetLocation:#{null}}")
    private String[] mongoReplicaSetLocation;

    @Value("${mongodb.mongoReplicaSetName:#{null}}")
    private String mongoReplicaSetName;

    @Override
    public MongoClient mongoClient() {
        logger.info("Mongo Configuration Values");
        //logger.info("mongodb.keyStoreKeyPassword : " + nullWrapper(this.mongoKeyStoreKeyPassword));
        //logger.info("mongodb.keyStorePassword : " + nullWrapper(this.mongoKeyStorePassword));
        logger.info("mongodb.keyStoreLocation : " + nullWrapper(this.mongoKeyStoreLocation));
        logger.info("mongodb.trustStoreLocation : " + nullWrapper(this.mongoTrustStoreLocation));
        //logger.info("mongodb.trustStorePassword : " + nullWrapper(this.mongoTrustStorePassword));
        logger.info("mongodb.databaseName : " + nullWrapper(this.mongoDatabaseName));
        logger.info("mongodb.username : " + nullWrapper(this.mongoUsername));
        //logger.info("mongodb.password : " + nullWrapper(this.mongoPassword));
        logger.info("mongodb.replicaSetLocation : " + nullWrapper(this.mongoReplicaSetLocation));
        logger.info("mongodb.mongoReplicaSetName : " + nullWrapper(this.mongoReplicaSetName));

        MongoClientParameters params = new MongoClientParameters()
                .setKeyStoreKeyPassword(this.mongoKeyStoreKeyPassword)
                .setKeyStoreLocation(this.mongoKeyStoreLocation)
                .setKeyStorePassword(this.mongoKeyStorePassword)
                .setTrustStoreLocation(this.mongoTrustStoreLocation)
                .setTrustStorePassword(this.mongoTrustStorePassword)
                .setDatabaseName(this.mongoDatabaseName)
                .setMongoUsername(this.mongoUsername)
                .setMongoPassword(this.mongoPassword)
                .setReplicaSetLocations(this.mongoReplicaSetLocation)
                .setReplicaSetName(this.mongoReplicaSetName);

        MongoClient client = null;
        if(this.mongoReplicaSetLocation != null && this.mongoReplicaSetLocation.length == 1 && this.mongoReplicaSetLocation[0].equalsIgnoreCase("localhost")) {
            logger.debug("Connecting to Local Mongo Instance");
            if (this.mongoUsername != null && !this.mongoUsername.isEmpty() &&
                    this.mongoPassword != null && !this.mongoPassword.isEmpty() &&
                    this.mongoDatabaseName != null && !this.mongoDatabaseName.isEmpty()) {
                client = MongoClientFactory.getLocalhostMongoClient(this.mongoDatabaseName, this.mongoUsername, this.mongoPassword);
            } else {
                client = MongoClientFactory.getLocalhostMongoClient();
            }
        } else if(this.mongoReplicaSetLocation == null){
            client = MongoClientFactory.getLocalhostMongoClient();
        }else {
            client = MongoClientFactory.getMongoClient(params);
        }
        return client;
    }

    @Override
    protected String getDatabaseName() {
        return this.mongoDatabaseName;
    }
}