import com.google.cloud.spanner.*;

public class InsertAndReadData {
    public static void main(String[] args) {
        String instanceId = "quickstart-instance";
        String databaseId = "quickstart-database";

        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();

        // Insert and Read Data
        DatabaseId db = DatabaseId.of(options.getProjectId(), instanceId, databaseId);
        DatabaseClient dbClient = spanner.getDatabaseClient(db);
        dbClient
                .readWriteTransaction()
                .run(transaction -> {
                    // Insert record.
                    String sql =
                            "INSERT INTO Customers (CustomerId, FirstName, LastName, MobileNumber) "
                                    + " VALUES (12, 'Timothy', 'Campbell', '9938718933')";
                    long rowCount = transaction.executeUpdate(Statement.of(sql));
                    System.out.printf("%d record inserted.\n", rowCount);
                    // Read newly inserted record.
                    sql = "SELECT FirstName, LastName, MobileNumber FROM Customers WHERE CustomerId = 12";
                    // We use a try-with-resource block to automatically release resources held by
                    // ResultSet.
                    try (ResultSet resultSet = transaction.executeQuery(Statement.of(sql))) {
                        while (resultSet.next()) {
                            System.out.printf(
                                    "%s %s %s\n",
                                    resultSet.getString("FirstName"), resultSet.getString("LastName"), resultSet.getString("MobileNumber"));
                        }
                    }
                    return null;
                });

    }
}