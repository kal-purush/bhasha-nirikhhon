import static com.mongodb.client.model.Filters.eq;
import org.bson.Document;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
public class App {
    public static void main(String[] args){
        System.out.println("connecting");
        String uri = "mongodb+srv://GotItAdmin:iNxcuCBzeUHt94Nn@cluster0.ef2wfei.mongodb.net/?retryWrites=true&w=majority";
        try(MongoClient client = MongoClients.create(uri);){
            MongoDatabase database = client.getDatabase("sample_mflix");
            MongoCollection<Document> collection = database.getCollection("comments");
            Document doc = collection.find(eq("name", "Mercedes Tyler")).first();
            System.out.println(doc.toJson());
            System.out.println("connected");
        }catch(Exception e){
            System.out.println("error");
        }
    }
}