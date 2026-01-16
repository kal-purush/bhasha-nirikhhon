package com.mongodb.week_5;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.gte;
import static java.util.Arrays.asList;

/**
 * Created by Alexander Kolobov on 4/24/2018.
 * https://docs.mongodb.com/manual/tutorial/aggregation-zip-code-data-set/
 */
public class ZipCodeAggregationTest {
    public static void main(String[] args) {
        MongoClient client = new MongoClient(new ServerAddress("localhost", 27017));

        MongoDatabase database = client.getDatabase("test");
        final MongoCollection<Document> collection = database.getCollection("zips");

//        The following aggregation operation returns all states with total population greater than 10 million
//        db.zipcodes.aggregate( [
//                { $group: { _id: "$state", totalPop: { $sum: "$pop" } } },
//        { $match: { totalPop: { $gte: 10*1000*1000 } } } ] )

        List<Document> pipeline = asList(new Document("$group", new Document("_id", "$state")
                .append("totalPop", new Document("$sum", "$pop"))),
                new Document("$match", new Document("totalPop", new Document("$gte", 10000000))));
//        Do above staff with Aggregates
        List<Bson> aggPipeline =asList(group("$state", sum("totalPop", "$pop")),
                match(gte("totalPop", 10000000)));
//        Also we can parse
        List<Document> parsePipeline = asList(Document.parse("{ $group: { _id: \"$state\", totalPop: { $sum: \"$pop\" } } },{ $match: { totalPop: { $gte: 10000000 } } }"));

        List<Document> results1 = collection.aggregate(pipeline).into(new ArrayList<>());
        List<Document> results2 = collection.aggregate(aggPipeline).into(new ArrayList<>());
        List<Document> results3 = collection.aggregate(parsePipeline).into(new ArrayList<>());

        for (Document cur : results1) {
            System.out.println(cur.toJson());
        }
        System.out.println("===========================");
        for (Document cur : results2) {
            System.out.println(cur.toJson());
        }
        System.out.println("===========================");
        for (Document cur : results3) {
            System.out.println(cur.toJson());
        }

    }
}