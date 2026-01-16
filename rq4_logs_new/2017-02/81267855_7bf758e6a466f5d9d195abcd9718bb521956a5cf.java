package helloPackage;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
/**
 * Created by Spartanrme on 2/9/2017.
 * Following the tutorial from: https://cloud.google.com/datastore/docs/reference/libraries
 */
public class dataStore {
    public static void main(String... args) throws Exception {
        // Instantiates a client
        Datastore datastore = DatastoreOptions.getDefaultInstance().getService();

        // The kind for the new entity
        String kind = "Task";
        // The name/ID for the new entity
        String name = "sampletask1";
        // The Cloud Datastore key for the new entity
        Key taskKey = datastore.newKeyFactory().setKind(kind).newKey(name);

        // Prepares the new entity
        Entity task = Entity.newBuilder(taskKey)
                .set("description", "Buy milk")
                .build();

        // Saves the entity
        datastore.put(task);
        System.out.printf("Saved %s: %s%n", task.getKey().getName(), task.getString("description"));
    }
}