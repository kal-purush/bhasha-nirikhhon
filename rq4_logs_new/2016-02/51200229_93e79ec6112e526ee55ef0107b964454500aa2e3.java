package org.apache.cmueller.samples.oracle;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static junit.framework.TestCase.assertEquals;

public class StoredProcedureTransactionTest {

    private Connection con;

    @Before
    public void setUp() throws SQLException {
        DriverManager.registerDriver (new oracle.jdbc.OracleDriver());
        con = DriverManager.getConnection("jdbc:oracle:thin:@192.168.178.28:1521:xe", "cmueller", "***");
        con.setAutoCommit(false);

        PreparedStatement statement = con.prepareStatement("CREATE OR REPLACE TABLE TEST (NAME VARCHAR2(100))");
        statement.execute();

        statement = con.prepareStatement(
            "CREATE OR REPLACE PROCEDURE ADD_TEST_ENTRY (NAME IN VARCHAR2) AS " +
            "BEGIN " +
                "INSERT INTO TEST VALUES (ADD_TEST_ENTRY.NAME); " +
            "END ADD_TEST_ENTRY;");
        statement.execute();

        con.commit();
    }

    @Test
    public void testCommit() throws SQLException {
        assertEquals(0, getTestTableRowCount());

        executeStoredProcedure();
        con.commit();

        assertEquals(1, getTestTableRowCount());
    }

    @Test
    public void testRollback() throws SQLException {
        assertEquals(0, getTestTableRowCount());

        executeStoredProcedure();
        con.rollback();

        assertEquals(0, getTestTableRowCount());
    }

    private void executeStoredProcedure() throws SQLException {
        CallableStatement statement = con.prepareCall("{call ADD_TEST_ENTRY(?)}");
        statement.setString(1, "CMUELLER");
        statement.execute();
    }

    private int getTestTableRowCount() throws SQLException {
        PreparedStatement statement = con.prepareStatement("SELECT COUNT(*) FROM TEST");
        return statement.executeUpdate();
    }
}