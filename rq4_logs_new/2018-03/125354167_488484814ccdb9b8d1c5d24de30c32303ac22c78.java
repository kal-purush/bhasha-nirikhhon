package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

public class AbstractDatabaseController {
    /**
     * The driver + filename of the sqlite database
     */
    private final String DATABASE;

    /**
     * creates a new DatabaseController of the given file.
     *
     * @param filename Name of the database file.
     */
    public AbstractDatabaseController(String filename) {
        this.DATABASE = "jdbc:sqlite:" + filename;
    }

    /**
     * Returns a new DatabaseConnection.
     *
     * @return Connection to the database {@link #DATABASE}.
     * @throws SQLException Error while connecting
     */
    private Connection connect() throws SQLException {
        return DriverManager.getConnection(DATABASE);
    }

    /**
     * Use this method to execute an insert, update, delete.
     *
     * @param s        Object to insert, update, delete
     * @param consumer Function which handles the insert update delete
     * @param <S>      Type of the Object to insert
     * @throws SQLException Something went wrong
     */
    public <S> void consumerWrapper(S s, DatabaseConsumer<S, Connection> consumer) throws SQLException {
        try (Connection connection = this.connect()) {
            if (connection == null) {
                throw new SQLException();
            } else {
                consumer.execute(s, connection);
            }
        }
    }

    /**
     * Use this method to execute an insert, update, delete.
     *
     * @param consumer Function which handles the insert update delete
     * @throws SQLException Something went wrong
     */
    public void consumerWrapper(DatabaseSpecifiedConsumer<Connection> consumer) throws SQLException {
        try (Connection connection = this.connect()) {
            if (connection == null) {
                throw new SQLException();
            } else {
                consumer.execute(connection);
            }
        }
    }

    /**
     * Use this Method to execute a select to get all entrys
     *
     * @param s        String of the table name
     * @param function function which performs a select
     * @param <S>      String to insert a table name
     * @param <R>      ResultSet as return type
     * @return Optional of ResultSet
     * @throws SQLException Something went wrong
     */
    public <S, R> Optional<R> functionWrapper(S s, DatabaseSelectFunction<S, Connection, R> function) throws SQLException {
        try (Connection connection = this.connect()) {
            if (connection == null) {
                throw new SQLException();
            } else {
                return Optional.of(function.execute(s, connection));
            }
        }
    }
}