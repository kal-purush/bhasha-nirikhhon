package model;

import java.sql.*;
import java.util.List;

public class UtilsDatabase {
    private static final String DB_NAME = "cetphhnl";
    private static final String DB_USER = "cetphhnl";
    private static final String DB_PASSWORD = "aAGfNp5piy3_W3aKzF05IuesJLQg44Li";
    private static final String DB_HOST = "dumbo.db.elephantsql.com";
    private static final String DB_PORT = "5432";
    private static final String JDBC_URL = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;
    private static Connection connection;

    public void connectToDatabase() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");

        connection = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
        System.out.println("Connected to the database.");
    }

    public void disconnectFromDatabase() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                System.out.println("Disconnected from the database.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void setSearchPath(String searchPath) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            // Execute the SET search_path query
            statement.execute("SET search_path TO " + searchPath);
        }
    }

    public void deleteAllDataFromTable(String tableName) {
        String updateQuery = "DELETE FROM " + tableName;

        try (
                var preparedStatement = connection.prepareStatement(updateQuery)
        ) {
            int rowsAffected = preparedStatement.executeUpdate();
            System.out.println(rowsAffected + " rows updated.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void setNULLToAllRecordsFromTable(String tableName, List<Object> primaryKeyList) {
        try {
            // Get metadata to retrieve column names
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getColumns(null, null, tableName, null);

            // Build the update query dynamically
            StringBuilder updateQuery = new StringBuilder("UPDATE " + tableName + " SET ");

            // Iterate through columns and append column names
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                if(!primaryKeyList.contains(columnName))
                    updateQuery.append(columnName).append(" = NULL, ");
            }

            // Remove the trailing comma and space
            updateQuery.setLength(updateQuery.length() - 2);

            // Execute the update query
            try (
                    PreparedStatement preparedStatement = connection.prepareStatement(updateQuery.toString())
            ) {
                int rowsAffected = preparedStatement.executeUpdate();
                System.out.println(rowsAffected + " rows updated.");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void resetDatabase() {
        try{
            connectToDatabase();
            setSearchPath("nba_project");
            deleteAllDataFromTable("player_game");
            deleteAllDataFromTable("team_game");
            deleteAllDataFromTable("game");
            setNULLToAllRecordsFromTable("team_stats", List.of("id_team_stats"));
            setNULLToAllRecordsFromTable("player_stats", List.of("id_player_stats"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            disconnectFromDatabase();
        }
    }
}