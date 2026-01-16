package demo_oracle_jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import oracle.jdbc.pool.OracleDataSource;

public class DBConnectionHandler {

    private String jdbcUrl;
    private String username;
    private String password;

    private Connection connection;
    private PreparedStatement prepStmt;
    private Statement stmt;
    private ResultSet rSet;

    public DBConnectionHandler(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;

        connection = null;
        prepStmt = null;
        rSet = null;
        stmt = null;
    }

    public void openConnection() throws SQLException {
        OracleDataSource ds = new OracleDataSource();
        ds.setURL(jdbcUrl);
        connection = ds.getConnection(username, password);
    }

    public String closeAll() {

        StringBuilder message = new StringBuilder("");

        if (rSet != null) {
            try {
                rSet.close();
            } catch (SQLException ex) {
                message.append(ex.getMessage());
                message.append("\n");
            }
            rSet = null;
        }

        if (prepStmt != null) {
            try {
                prepStmt.close();
            } catch (SQLException ex) {
                message.append(ex.getMessage());
                message.append("\n");
            }
            prepStmt = null;
        }

        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ex) {
                message.append(ex.getMessage());
                message.append("\n");
            }
            prepStmt = null;
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ex) {
                message.append(ex.getMessage());
                message.append("\n");
            }
            connection = null;
        }
        return message.toString();
    }
    
    public ResultSet selectAllFromTable (String tableName) throws SQLException {
        Statement stmt = connection.createStatement();
        rSet = stmt.executeQuery("SELECT * FROM " + tableName);
        return rSet;
    }

    public void insertIntoDistribuidor (int distribuidorID, int idPessoa, String dataContrato) throws SQLException {
        prepStmt = connection.prepareStatement("INSERT INTO Distribuidor (distribuidorID, idPessoa, dataContrato) " +
                "Values ( ?, ?, ?)");
        prepStmt.setInt(1, distribuidorID);
        prepStmt.setInt(2, idPessoa);
        prepStmt.setDate(3, new java.sql.Date(Date.parse(dataContrato)));

        prepStmt.execute();
    }

    public void deleteFromDistribuidorWithID (int distribuidorID) throws SQLException {
        prepStmt = connection.prepareStatement("DELETE FROM Distribuidor WHERE distribuidorID = ?");
        prepStmt.setInt(1, distribuidorID);

        prepStmt.execute();
    }


}