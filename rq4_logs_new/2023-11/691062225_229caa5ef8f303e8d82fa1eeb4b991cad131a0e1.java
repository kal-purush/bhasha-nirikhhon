package org.lab4;

import org.neo4j.driver.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Neo4jApplication {

    private final Driver driver;
    private final Random random = new Random();

    public Neo4jApplication(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    public void close() {
        driver.close();
    }

    //deleteAllDataFromDb
    public void deleteAllDataFromDb() {
        try (Session session = driver.session()) {
            session.writeTransaction(tx -> {
                tx.run("MATCH (n) DETACH DELETE n");
                return null;
            });
        }
    }

    //generateRandomDatabase
    public void generateRandomDatabase(int numberOfCustomers, int numberOfItems, int numberOfOrders) {
        try (Session session = driver.session()) {
            // Generate Items
            for (int i = 0; i < numberOfItems; i++) {
                int cost = random.nextInt(1000) + 100; // Items cost between 100 and 1100
                String itemName = "Product" + (i + 1);
                session.writeTransaction(tx -> {
                    tx.run("CREATE (:Item {name: $name, cost: $cost})",
                            java.util.Map.of("name", itemName, "cost", cost));
                    return 1;
                });
            }

            // Generate Customers and Orders
            for (int i = 0; i < numberOfCustomers; i++) {
                String customerName = "Customer" + (i + 1);
                // Create Customer
                session.writeTransaction(tx -> {
                    tx.run("CREATE (:Customer {name: $name})",
                            java.util.Map.of("name", customerName));
                    return 1;
                });

                for (int j = 0; j < numberOfOrders; j++) {
                    String orderName = "Order" + (i + 1) + "_" + (j + 1);
                    session.writeTransaction(tx -> {
                        tx.run("MATCH (c:Customer {name: $customerName}) " +
                                        "CREATE (c)-[:MADE]->(:Order {name: $orderName})",
                                java.util.Map.of("customerName", customerName, "orderName", orderName));
                        return 1;
                    });

                    int itemsPerOrder = random.nextInt(5) + 1;
                    for (int k = 0; k < itemsPerOrder; k++) {
                        session.writeTransaction(tx -> {
                            tx.run("MATCH (o:Order {name: $orderName}), (i:Item) " +
                                            "WITH o, i, rand() AS r ORDER BY r LIMIT 1 " +
                                            "CREATE (o)-[:INCLUDES]->(i)",
                                    java.util.Map.of("orderName", orderName));
                            return 1;
                        });
                    }
                }
            }

            // Generate Views
            for (int i = 0; i < numberOfCustomers * 2; i++) {
                session.writeTransaction(tx -> {
                    tx.run("MATCH (c:Customer), (i:Item) " +
                            "WITH c, i, rand() AS r WHERE r < 0.5 " +
                            "CREATE (c)-[:VIEWED]->(i)");
                    return 1;
                });
            }
        }
    }




    // findAllOrdersByCustomer
    public List<Map<String, Object>> findAllOrdersByCustomer(int customerId) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (c:Customer)-[:MADE]->(o:Order) WHERE id(c) = $customerId " +
                                "RETURN o",
                        java.util.Map.of("customerId", customerId));
                return result.list(record -> record.get("o").asMap());
            });
        }
    }

    // findItemsInOrder
    public List<Map<String, Object>> findItemsInOrder(int orderId) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (o:Order)-[:INCLUDES]->(i:Item) WHERE id(o) = $orderId " +
                                "RETURN i",
                        java.util.Map.of("orderId", orderId));
                return result.list(record -> record.get("i").asMap());
            });
        }
    }

    // calculateOrderCost
    public int calculateOrderCost(int orderId) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (o:Order)-[:INCLUDES]->(i:Item) WHERE id(o) = $orderId " +
                                "RETURN sum(i.cost) AS totalCost",
                        java.util.Map.of("orderId", orderId));
                return result.single().get("totalCost").asInt();
            });
        }
    }

    // findItemsBoughtByCustomer
    public List<Map<String, Object>> findItemsBoughtByCustomer(int customerId) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (c:Customer)-[:MADE]->(:Order)-[:INCLUDES]->(i:Item) WHERE id(c) = $customerId " +
                                "RETURN i",
                        java.util.Map.of("customerId", customerId));
                return result.list(record -> record.get("i").asMap());
            });
        }
    }

    // countItemsBoughtByCustomer
    public int countItemsBoughtByCustomer(int customerId) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (c:Customer)-[:MADE]->(:Order)-[:INCLUDES]->(i:Item) WHERE id(c) = $customerId " +
                                "RETURN count(i) AS itemCount",
                        java.util.Map.of("customerId", customerId));
                return result.single().get("itemCount").asInt();
            });
        }
    }

    // findTotalCostForCustomer
    public int findTotalCostForCustomer(int customerId) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (c:Customer)-[:MADE]->(:Order)-[:INCLUDES]->(i:Item) WHERE id(c) = $customerId " +
                                "RETURN sum(i.cost) AS totalSpent",
                        java.util.Map.of("customerId", customerId));
                return result.single().get("totalSpent").asInt();
            });
        }
    }

    // findItemCountAndSort
    public List<Map<String, Object>> findItemCountAndSort() {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (:Order)-[:INCLUDES]->(i:Item) " +
                        "RETURN i, count(i) AS purchases ORDER BY purchases DESC");
                return result.list(record -> {
                    Map<String, Object> itemAndCount = new HashMap<>();
                    itemAndCount.put("item", record.get("i").asMap());
                    itemAndCount.put("purchases", record.get("purchases").asInt());
                    return itemAndCount;
                });
            });
        }
    }

    // findItemsViewedByCustomer
    public List<Map<String, Object>> findItemsViewedByCustomer(int customerId) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (c:Customer)-[:VIEWED]->(i:Item) WHERE id(c) = $customerId " +
                                "RETURN i",
                        java.util.Map.of("customerId", customerId));
                return result.list(record -> record.get("i").asMap());
            });
        }
    }

    // findItemsBoughtWithItem
    public List<Map<String, Object>> findItemsBoughtWithItem(int itemId) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (:Order)-[:INCLUDES]->(i:Item) WHERE id(i) = $itemId " +
                                "WITH i MATCH (i)<-[:INCLUDES]-(o:Order)-[:INCLUDES]->(other:Item) " +
                                "RETURN DISTINCT other",
                        java.util.Map.of("itemId", itemId));
                return result.list(record -> record.get("other").asMap());
            });
        }
    }

    public List<Map<String, Object>> findCustomersWhoBoughtItem(int itemId) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (c:Customer)-[:MADE]->(:Order)-[:INCLUDES]->(i:Item) WHERE id(i) = $itemId " +
                                "RETURN c",
                        java.util.Map.of("itemId", itemId));
                return result.list(record -> record.get("c").asMap());
            });
        }
    }

    // findViewedButNotBoughtItems
    public List<Map<String, Object>> findViewedButNotBoughtItems(int customerId) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (c:Customer)-[:VIEWED]->(i:Item) WHERE id(c) = $customerId " +
                                "AND NOT (c)-[:MADE]->(:Order)-[:INCLUDES]->(i) " +
                                "RETURN i",
                        java.util.Map.of("customerId", customerId));
                return result.list(record -> record.get("i").asMap());
            });
        }
    }

    public static void main(String[] args) {
        Neo4jApplication app = new Neo4jApplication("bolt://localhost:7687", "neo4j", "password");

//        System.out.println("Deleting all data from the database...");
//        app.deleteAllDataFromDb();
//
//        System.out.println("Generating random database with customers, orders, and items...");
//        app.generateRandomDatabase(10, 50, 5);

        int exampleOrderId = 66;
        List<Map<String, Object>> itemsInOrder = app.findItemsInOrder(exampleOrderId);
        printLimitedList(itemsInOrder, "Finding items in order with ID " + exampleOrderId);

        int orderCost = app.calculateOrderCost(exampleOrderId);
        System.out.println("\nCalculating total cost for order ID " + exampleOrderId + ":\nTotal Cost: " + orderCost);

        int exampleCustomerId = 61;
        List<Map<String, Object>> allOrdersByCustomer = app.findAllOrdersByCustomer(exampleCustomerId);
        printLimitedList(allOrdersByCustomer, "Finding all orders made by customer with ID " + exampleCustomerId);

        List<Map<String, Object>> itemsBoughtByCustomer = app.findItemsBoughtByCustomer(exampleCustomerId);
        printLimitedList(itemsBoughtByCustomer, "Finding items bought by customer with ID " + exampleCustomerId);

        int itemCount = app.countItemsBoughtByCustomer(exampleCustomerId);
        System.out.println("\nCounting items bought by customer with ID " + exampleCustomerId + ":\nItem Count: " + itemCount);

        int totalSpent = app.findTotalCostForCustomer(exampleCustomerId);
        System.out.println("\nCalculating total cost for customer with ID " + exampleCustomerId + ":\nTotal Spent: " + totalSpent);

        List<Map<String, Object>> itemCountAndSort = app.findItemCountAndSort();
        printLimitedList(itemCountAndSort, "Finding items by purchase count and sorting them");

        List<Map<String, Object>> itemsViewedByCustomer = app.findItemsViewedByCustomer(exampleCustomerId);
        printLimitedList(itemsViewedByCustomer, "Finding items viewed by customer with ID " + exampleCustomerId);

        int exampleItemId = 1583;
        List<Map<String, Object>> itemsBoughtWithItem = app.findItemsBoughtWithItem(exampleItemId);
        printLimitedList(itemsBoughtWithItem, "Finding items bought with item ID " + exampleItemId);

        List<Map<String, Object>> customersWhoBoughtItem = app.findCustomersWhoBoughtItem(exampleItemId);
        printLimitedList(customersWhoBoughtItem, "Finding customers who bought item with ID " + exampleItemId);

        List<Map<String, Object>> viewedButNotBoughtItems = app.findViewedButNotBoughtItems(exampleCustomerId);
        printLimitedList(viewedButNotBoughtItems, "Finding items that were viewed but not bought by customer with ID " + exampleCustomerId);

        app.close();
    }

    private static void printLimitedList(List<Map<String, Object>> list, String title) {
        System.out.println("\n" + title + ":");
        int size = list.size();
        int limit = 5;

        for (int i = 0; i < Math.min(limit, size); i++) {
            System.out.println(list.get(i));
        }

        if (size > 2 * limit) {
            System.out.println("...");
        }

        for (int i = Math.max(size - limit, limit); i < size; i++) {
            System.out.println(list.get(i));
        }
    }

}