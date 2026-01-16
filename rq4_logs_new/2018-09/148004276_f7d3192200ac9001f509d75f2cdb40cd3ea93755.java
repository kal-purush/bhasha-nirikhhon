package com.couchbase.devguide;

import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

/**
 * Example of MapReduce View Query in Java for the Couchbase Developer Guide.
 */

/**
 * for more
 * https://docs.couchbase.com/java-sdk/2.6/view-queries-with-sdk.html#querying-views-through-the-java-sdk
 * https://www.baeldung.com/couchbase-query-mapreduce-view
 * https://docs.couchbase.com/java-sdk/2.6/start-using-sdk.html
 * http://docs.couchbase.com/sdk-api/couchbase-java-client-2.6.2/
 */

public class MRViewQuery extends ConnectionBase {

    @Override
    protected void doWork() {
        ViewResult result1 = bucket.query(ViewQuery.from("aatest", "aatest"));
        for (ViewRow row : result1) {
            System.out.println(row); //prints the row
            System.out.println(row.document().content()); //retrieves the doc and prints content
        }

        ViewQuery q2 = ViewQuery.from("aatest", "aatest")
            .limit(5) // Limit to 5 results
            .startKey("cloud_service_couchbase_1")
            .endKey("cloud_service_couchbase_100");

        ViewResult result2 = bucket.query(q2);
        for (ViewRow row : result2) {
            System.out.println(row);
        }
    }

    public static void main(String[] args) {
        new MRViewQuery().execute();
    }
}