import static com.mongodb.client.model.Filters.eq;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.*;
import LoginUseCase.LoginDBGateway;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import java.util.ArrayList;

public class DatabaseUser implements LoginDBGateway{
    private final ConnectionString mongoURI;
    private MongoCollection<Document> usersCollection;

    public DatabaseUser(String uri){
        this.mongoURI = new ConnectionString(uri);
        CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
        CodecRegistry codecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                pojoCodecRegistry);
        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(mongoURI)
                        .codecRegistry(codecRegistry)
                        .serverApi(ServerApi.builder()
                                .version(ServerApiVersion.V1).build())
                        .build();
        try(MongoClient mongoClient = MongoClients.create(settings)){
            MongoDatabase mainDB = mongoClient.getDatabase("main");
            this.usersCollection = mainDB.getCollection("users");
        }catch(Exception e) {
            System.out.println(e);
        }
    }

    @Override
    public boolean validateAndLogin(String email, String password) {
        System.out.println("here");
        Bson filter = Filters.and(Filters.eq("email", email),Filters.eq("password", password));
        boolean loggedIn = (boolean) usersCollection.find(filter).first().get("loggedIn");
        if(usersCollection.countDocuments(filter) == 1 && loggedIn){
            try{
                Bson updateLogIn = Updates.set("loggedIn", true);
                usersCollection.updateOne(filter, updateLogIn);
                return true;
            }catch(Exception loginFailure){
                return false;
            }
        };
        return false;
    }
}